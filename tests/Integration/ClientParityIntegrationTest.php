<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Services\ServiceError;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * Live-server coverage for the P1 client-API parity features (#16–#32) against the Docker fixtures.
 */
final class ClientParityIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    private function client(): NatsClient
    {
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        return $client;
    }

    /**
     * Pumps a client's read loop in a background fiber until the returned canceller is cancelled.
     */
    private function pump(NatsClient $client): DeferredCancellation
    {
        $canceller = new DeferredCancellation();
        async(static function () use ($client, $canceller): void {
            $cancellation = $canceller->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $client->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
                // Stopped once the assertion completed.
            } catch (\Throwable) {
                // Connection torn down at test end.
            }
        });

        return $canceller;
    }

    /**
     * #17 — a delivered message replies to its own reply subject via respond().
     */
    public function testRespondHelperRepliesToRequester(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.respond.' . bin2hex(random_bytes(4));
        $server = $this->client();
        $client = $this->client();

        $server->subscribe($subject, static function (NatsMessage $message): void {
            $message->respondWithHeaders('pong', ['X-Echo' => $message->payload])->await();
        })->await();
        $server->flush()->await();

        $pump = $this->pump($server);
        try {
            $reply = $client->request($subject, 'ping', 3000)->await();
        } finally {
            $pump->cancel();
        }

        self::assertSame('pong', $reply->payload);
        $headers = \IDCT\NATS\Core\NatsHeaders::fromWireBlock($reply->rawHeaders);
        self::assertSame('ping', $headers['X-Echo'] ?? null);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * #21 — requestMany collects multiple replies and stops at maxResponses.
     */
    public function testRequestManyCollectsMultipleReplies(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.scatter.' . bin2hex(random_bytes(4));
        $server = $this->client();
        $client = $this->client();

        // One responder emits three replies to the requester's inbox.
        $server->subscribe($subject, static function (NatsMessage $message) use ($server): void {
            if ($message->replyTo === null) {
                return;
            }
            foreach (['a', 'b', 'c'] as $part) {
                $server->publish($message->replyTo, $part)->await();
            }
        })->await();
        $server->flush()->await();

        $pump = $this->pump($server);
        try {
            $replies = $client->requestMany($subject, 'go', null, 3, 3000, 500)->await();
        } finally {
            $pump->cancel();
        }

        $payloads = array_map(static fn(NatsMessage $m): string => $m->payload, $replies);
        sort($payloads);
        self::assertSame(['a', 'b', 'c'], $payloads);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * #22 — the connection listener observes Connected then Closed.
     */
    public function testConnectionLifecycleListenerObservesConnectAndClose(): void
    {
        $this->requireIntegrationEnabled();

        $events = [];
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            connectionListener: static function (ConnectionEvent $event) use (&$events): void {
                $events[] = $event;
            },
        ));

        $client->connect()->await();
        $client->disconnect()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $events);
    }

    /**
     * #24 — a token provider supplies credentials on connect (token-auth server).
     */
    public function testDynamicTokenProviderAuthenticates(): void
    {
        $this->requireIntegrationEnabled();

        $calls = 0;
        $token = $this->integrationToken();
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationTokenServerUrl()],
            tokenProvider: static function () use (&$calls, $token): string {
                $calls++;

                return $token;
            },
        ));

        $client->connect()->await();
        // A round trip proves the connection is authenticated and usable.
        $subject = 'it.tok.' . bin2hex(random_bytes(4));
        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m->payload;
        })->await();
        $client->publish($subject, 'hi')->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertSame('hi', $received);
        self::assertGreaterThanOrEqual(1, $calls);

        $client->disconnect()->await();
    }

    /**
     * #16 — optimistic-concurrency publish expectations: match succeeds, mismatch fails.
     */
    public function testPublishExpectationsEnforceLastSequence(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.exp';
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $first = $js->publish($subject, 'one')->await();
        // Correct expectation: appends at the known last sequence.
        $second = $js->publish($subject, 'two', expectedLastSequence: $first->seq)->await();
        self::assertSame($first->seq + 1, $second->seq);

        // Stale expectation is rejected by the server.
        $threw = false;
        try {
            $js->publish($subject, 'three', expectedLastSequence: $first->seq)->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'A stale expectedLastSequence must be rejected');

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #20 — deleteMessage removes a stored message by sequence.
     */
    public function testDeleteMessageRemovesStoredMessage(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.del';
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $ack = $js->publish($subject, 'gone-soon')->await();
        self::assertSame('gone-soon', $js->getStreamMessage($stream, $ack->seq)->await()->payload);

        self::assertTrue($js->deleteMessage($stream, $ack->seq)->await());

        $threw = false;
        try {
            $js->getStreamMessage($stream, $ack->seq)->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'The deleted sequence must no longer be retrievable');

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #18 + #30 — ackSync double-acks a pulled message and messageMetadata exposes the $JS.ACK tuple.
     */
    public function testAckSyncAndMessageMetadataOnPulledMessage(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.meta';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        $js->publish($subject, 'm1')->await();
        $js->publish($subject, 'm2')->await();

        $messages = $js->fetchBatch($stream, $consumer, 1, 2000)->await();
        $message = $messages[0];

        $meta = $js->messageMetadata($message);
        self::assertSame($stream, $meta->stream);
        self::assertSame($consumer, $meta->consumer);
        self::assertSame(1, $meta->streamSequence);
        self::assertSame(1, $meta->numDelivered);
        self::assertGreaterThanOrEqual(1, $meta->numPending);
        self::assertGreaterThan(0, $meta->timestampNanos);

        // Double-ack resolves only when the server confirms the ack was recorded.
        $js->ackSync($message, 2000)->await();

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #32 — a pull consumer's handle loop stops promptly when stop() is signalled from the handler.
     */
    public function testPullConsumerStopHaltsLoop(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.stop';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        for ($i = 1; $i <= 5; $i++) {
            $js->publish($subject, (string) $i)->await();
        }

        $iter = $js->pullConsumer($stream, $consumer)->setBatching(1)->setExpiresMs(700);
        $seen = [];
        $total = $iter->handle(function (NatsMessage $message, $ctx) use (&$seen, $iter): void {
            $seen[] = $message->payload;
            $ctx->ack($message)->await();
            if (count($seen) === 2) {
                $iter->stop();
            }
        })->await();

        self::assertSame(2, $total, 'stop() must halt the loop after the second message');

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #52 — connection accessors (connectedUrl, maxPayload, RTT, statistics) against a live server.
     */
    public function testConnectionAccessorsLive(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationServerUrl();
        $client = new NatsClient(new NatsOptions(servers: [$url]));
        $client->connect()->await();

        self::assertSame($url, $client->connectedUrl());
        self::assertGreaterThan(0, $client->maxPayload());

        $rtt = $client->rtt()->await();
        self::assertGreaterThan(0.0, $rtt);

        $subject = 'it.stats.' . bin2hex(random_bytes(4));
        $client->subscribe($subject, static function (NatsMessage $m): void {})->await();
        $client->flush()->await();
        $client->publish($subject, 'payload-12')->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while ($client->statistics()->inMsgs < 1) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        $stats = $client->statistics();
        self::assertGreaterThanOrEqual(1, $stats->outMsgs);
        self::assertGreaterThanOrEqual(1, $stats->inMsgs);

        $client->disconnect()->await();
    }

    /**
     * #46 — a bad credential fails fast with AuthenticationException, not an exhausted reconnect loop.
     */
    public function testAuthenticationErrorFailsFast(): void
    {
        $this->requireIntegrationEnabled();

        // Reconnect ENABLED with several attempts; an auth failure must still return promptly.
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationTokenServerUrl()],
            reconnectEnabled: true,
            maxReconnectAttempts: 5,
            reconnectDelayMs: 200,
            token: 'definitely-the-wrong-token',
        ));

        $start = microtime(true);
        $threw = false;
        try {
            $client->connect()->await();
        } catch (\IDCT\NATS\Exception\AuthenticationException) {
            $threw = true;
        }
        $elapsed = microtime(true) - $start;

        self::assertTrue($threw, 'A bad token must raise AuthenticationException');
        // Fast-fail: well under the time 5 reconnect attempts (with 200ms+ backoff each) would take.
        self::assertLessThan(2.0, $elapsed);
    }

    /**
     * #33 + #41 — KV getRevision reads a historical revision and history() returns all revisions.
     */
    public function testKeyValueGetRevisionAndHistory(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'hi' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create(['history' => 10])->await();

        $r1 = $kv->put('color', 'red')->await();
        $kv->put('color', 'green')->await();
        $kv->put('color', 'blue')->await();

        // getRevision reads the value stored at the first revision.
        $old = $kv->getRevision('color', $r1->seq)->await();
        self::assertNotNull($old);
        self::assertSame('red', $old->value);

        // history returns every revision, oldest first.
        $history = $kv->history('color')->await();
        self::assertSame(['red', 'green', 'blue'], array_map(static fn($e): ?string => $e->value, $history));

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #34 — compare-and-delete: a stale expected revision is rejected, the current one succeeds.
     */
    public function testKeyValueCompareAndDelete(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'cd' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create(['history' => 10])->await();

        $kv->put('lock', 'v1')->await();
        $current = $kv->put('lock', 'v2')->await();

        // A stale revision is rejected.
        $threw = false;
        try {
            $kv->delete('lock', null, $current->seq - 1)->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'compare-and-delete must reject a stale revision');
        self::assertNotNull($kv->get('lock')->await()?->value);

        // The current revision succeeds: the key now reads back as a tombstone (DEL, null value).
        $kv->delete('lock', null, $current->seq)->await();
        self::assertSame('DEL', $kv->get('lock')->await()->operation);

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #35 — streamNames() and consumerNames() list names without the full info payload.
     */
    public function testStreamAndConsumerNames(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.names';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        self::assertContains($stream, $js->streamNames()->await());
        self::assertContains($stream, $js->streamNames($subject)->await());
        self::assertSame([$consumer], $js->consumerNames($stream)->await());

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #36 — getLastMessageForSubject returns the most recent message for a subject (leader path).
     */
    public function testGetLastMessageForSubjectLive(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $base = 'it.' . strtolower($stream);
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$base . '.>'])->await();

        $js->publish($base . '.a', 'first-a')->await();
        $js->publish($base . '.b', 'only-b')->await();
        $js->publish($base . '.a', 'second-a')->await();

        $lastA = $js->getLastMessageForSubject($stream, $base . '.a')->await();
        self::assertSame('second-a', $lastA->payload);
        self::assertSame($base . '.a', $lastA->subject);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #44 — createOrUpdateStream upserts: creates first, then updates the existing stream's subjects.
     */
    public function testCreateOrUpdateStreamUpserts(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $base = 'it.' . strtolower($stream);
        $client = $this->client();
        $js = $client->jetStream();

        $created = $js->createOrUpdateStream($stream, [$base . '.one'])->await();
        self::assertSame([$base . '.one'], $created->subjects);

        // Second call on the existing stream updates its subject set (no "already in use" error).
        $updated = $js->createOrUpdateStream($stream, [$base . '.one', $base . '.two'])->await();
        self::assertSame([$base . '.one', $base . '.two'], $updated->subjects);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #19 — KV createKey is exclusive: first create wins, a second throws.
     */
    public function testKeyValueCreateKeyIsExclusive(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'cr' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $ack = $kv->createKey('token', 'first')->await();
        self::assertGreaterThanOrEqual(1, $ack->seq);

        $threw = false;
        try {
            $kv->createKey('token', 'second')->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'createKey on an existing live key must throw');

        self::assertSame('first', $kv->get('token')->await()?->value);

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #25 — keys() returns live key names, excluding deleted keys.
     */
    public function testKeyValueKeysListsLiveKeys(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ky' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $kv->put('alpha', '1')->await();
        $kv->put('beta', '2')->await();
        $kv->put('gamma', '3')->await();
        $kv->delete('beta')->await();

        $keys = $kv->keys()->await();
        sort($keys);
        self::assertSame(['alpha', 'gamma'], $keys);
        self::assertSame($keys, $kv->listKeys()->await());

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #26 — watch options replay history and signal end-of-initial-data, suppressing deletes.
     */
    public function testKeyValueWatchOptionsReplayHistoryAndSignalCaughtUp(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'wo' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $kv->put('one', 'A')->await();
        $kv->put('two', 'B')->await();
        $kv->delete('two')->await();

        $seen = [];
        $caughtUp = false;
        $sid = $kv->watch(
            static function ($entry) use (&$seen): void {
                $seen[$entry->key] = $entry->value;
            },
            '>',
            new KeyWatchOptions(ignoreDeletes: true, onCaughtUp: static function () use (&$caughtUp): void {
                $caughtUp = true;
            }),
        )->await();

        $cancellation = new TimeoutCancellation(4.0);
        try {
            while (!$caughtUp) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertTrue($caughtUp, 'onCaughtUp must fire once the initial replay completes');
        // Last-per-subject replay delivers the live value of 'one'; 'two' is a delete and is suppressed.
        self::assertSame('A', $seen['one'] ?? null);
        self::assertArrayNotHasKey('two', $seen);

        $client->unsubscribe($sid)->await();
        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #28 — ObjectStore updateMeta renames an object without re-uploading its bytes.
     */
    public function testObjectStoreUpdateMetaRenames(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ob' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $payload = 'the-quick-brown-fox';
        $store->put('logo.bin', $payload, ['team' => 'design'])->await();

        $info = $store->updateMeta('logo.bin', 'brand.bin', ['team' => 'brand'])->await();
        self::assertSame('brand.bin', $info->name);
        self::assertSame(['team' => 'brand'], $info->metadata);

        // The renamed object resolves and its bytes survived the rename (no re-upload).
        $fetched = $store->get('brand.bin')->await();
        self::assertNotNull($fetched);
        self::assertSame($payload, $fetched->data);
        // The old name is tombstoned (gone): its latest meta is a delete marker.
        $old = $store->info('logo.bin')->await();
        self::assertNotNull($old);
        self::assertTrue($old->deleted);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #27 — a service endpoint handler replies with a custom ServiceError (code/description/body).
     */
    public function testServiceHandlerRepliesWithCustomError(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.svcerr.' . bin2hex(random_bytes(4));
        $serviceClient = $this->client();
        $requester = $this->client();

        $service = $serviceClient->service('errsvc', '1.0.0')
            ->addEndpoint('fail', $subject, static function (NatsMessage $message): string {
                throw new ServiceError(429, 'Rate limited', '{"retry_after":5}');
            }, queueGroup: null);
        $service->start()->await();

        $pump = $this->pump($serviceClient);
        $reply = null;
        try {
            // Retry to absorb the brief window before the endpoint SUB is registered server-side.
            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $reply = $requester->request($subject, 'go', 2000)->await();
                    break;
                } catch (\IDCT\NATS\Exception\NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }
                    \Amp\delay(0.1);
                }
            }
        } finally {
            $pump->cancel();
        }

        self::assertInstanceOf(NatsMessage::class, $reply);
        $headers = \IDCT\NATS\Core\NatsHeaders::fromWireBlock($reply->rawHeaders);
        self::assertSame('Rate limited', $headers['Nats-Service-Error'] ?? null);
        self::assertSame('429', $headers['Nats-Service-Error-Code'] ?? null);
        self::assertSame('{"retry_after":5}', $reply->payload);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * #31 — the WebSocket transport carries core pub/sub and JetStream over ws://.
     */
    public function testWebSocketTransportCarriesPubSubAndJetStream(): void
    {
        $this->requireIntegrationEnabled();

        $options = new NatsOptions(servers: [$this->integrationWsServerUrl()]);
        $client = new NatsClient($options, new WebSocketTransport($options));
        $client->connect()->await();

        // Core pub/sub round trip over WebSocket frames.
        $subject = 'it.ws.' . bin2hex(random_bytes(4));
        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m->payload;
        })->await();
        $client->publish($subject, 'ws-hello')->await();

        $cancellation = new TimeoutCancellation(4.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }
        self::assertSame('ws-hello', $received);

        // JetStream publish + read-back over the same WebSocket connection.
        $stream = 'IT_WS_' . strtoupper(bin2hex(random_bytes(3)));
        $jsSubject = 'it.ws.' . strtolower($stream) . '.events';
        $js = $client->jetStream();
        $js->createStream($stream, [$jsSubject])->await();
        $ack = $js->publish($jsSubject, '{"over":"websocket"}')->await();
        self::assertGreaterThanOrEqual(1, $ack->seq);
        self::assertSame('{"over":"websocket"}', $js->getStreamMessage($stream, $ack->seq)->await()->payload);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }
}
