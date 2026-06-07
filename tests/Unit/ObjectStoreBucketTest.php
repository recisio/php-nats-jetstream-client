<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectData;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ObjectStoreBucketTest extends TestCase
{
    /** URL-safe base64 (with padding), matching the official Object Store meta-subject encoding. */
    private function encodeName(string $name): string
    {
        return strtr(base64_encode($name), '+/', '-_');
    }

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    /** @param array<string,mixed> $extra */
    private function metaGetResponse(string $name, array $extra): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);

        return (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $this->encodeName($name),
                'seq' => 2,
                'data' => base64_encode((string) json_encode($meta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);
    }

    private function notFound(): string
    {
        return '{"error":{"code":404,"description":"message not found"}}';
    }

    /**
     * Builds a Direct Get reply (HMSG) whose body is the raw meta JSON, as list() now reads it.
     *
     * @param array<string,mixed> $extra
     */
    private function directMetaReply(string $name, array $extra, int $sid): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);
        $body = (string) json_encode($meta, JSON_THROW_ON_ERROR);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.M." . $this->encodeName($name) . "\r\nNats-Sequence: 2\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($body), $hdrs, $body);
    }

    /** Builds a Direct Get reply (HMSG) for a single object chunk (raw bytes body on the NUID subject). */
    private function directChunkReply(string $nuid, string $payload, int $sid): string
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.C.{$nuid}\r\nNats-Sequence: 3\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($payload), $hdrs, $payload);
    }

    /**
     * Converts a metaGetResponse() STREAM.MSG.GET envelope into the equivalent Direct Get reply
     * (HMSG with the raw meta JSON as the body), as info()/get()/getToCallback() now read it.
     */
    private function directReplyFromEnvelope(string $envelope, int $sid): string
    {
        /** @var array{message: array{seq?: int, data?: string}} $decoded */
        $decoded = json_decode($envelope, true, 512, JSON_THROW_ON_ERROR);
        $metaJson = (string) base64_decode((string) ($decoded['message']['data'] ?? ''), true);
        $seq = (int) ($decoded['message']['seq'] ?? 1);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Sequence: {$seq}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($metaJson), $hdrs, $metaJson);
    }

    /** Builds a Direct Get status-only reply (HMSG) such as a 404 miss or a non-404 error. */
    private function directStatusReply(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\nDescription: {$description}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    private function pubAck(int $seq): string
    {
        return sprintf('{"stream":"OBJ_assets","seq":%d,"duplicate":false}', $seq);
    }

    /**
     * Verifies Object Store bucket create maps to a stream with chunk+meta subjects and rollup.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"OBJ_assets","subjects":["$O.assets.C.>","$O.assets.M.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $created = $bucket->create()->await();
        $deleted = $bucket->deleteBucket()->await();

        self::assertSame('OBJ_assets', $created->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.OBJ_assets', $transport->writes[3]);
        self::assertStringContainsString('"allow_rollup_hdrs":true', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.OBJ_assets', $transport->writes[6]);
    }

    /**
     * Verifies put writes chunks under a NUID subject and rollup meta under the encoded name subject.
     */
    public function testPutUsesEncodedMetaSubjectAndNuidChunks(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // 1) existing-object lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // 2) chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            // 3) meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $stored = $client->jetStream()->objectStore('assets')
            ->put('logo.txt', 'hello', ['content-type' => 'text/plain'])->await();

        self::assertInstanceOf(ObjectInfo::class, $stored);
        self::assertSame('logo.txt', $stored->name);
        self::assertSame(5, $stored->size);
        self::assertSame(1, $stored->chunks);
        self::assertNotSame('', $stored->nuid);
        self::assertSame($this->digestOf('hello'), $stored->digest);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('PUB $O.assets.C.' . $stored->nuid . ' ', $writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('Nats-Rollup:sub', $writes);
    }

    public function testPutStreamReChunksAndComputesDigestIncrementally(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // lookup (sid 1, concurrent), two chunk acks (sid 2, 3), meta ack (sid 4)
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // 3-byte chunks: a single 'hello' block re-chunks to 'hel' + 'lo' (2 chunks).
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 3);
        $blocks = ['hello'];
        $index = 0;
        $stored = $store->putStream('big.txt', static function () use (&$index, $blocks): ?string {
            return $blocks[$index++] ?? null;
        })->await();

        self::assertSame(5, $stored->size);
        self::assertSame(2, $stored->chunks);
        self::assertSame($this->digestOf('hello'), $stored->digest);

        $writes = implode('||', $transport->writes);
        self::assertSame(2, substr_count($writes, 'PUB $O.assets.C.' . $stored->nuid . ' '));
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('big.txt'), $writes);
    }

    /**
     * Verifies an empty object stores zero chunks and publishes no chunk message (only meta).
     */
    public function testPutStoresEmptyObjectWithZeroChunks(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // With no chunk to await first, the meta publish is issued before the concurrent lookup:
            // 1) meta publish ack (sid 1)
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            // 2) existing-object lookup -> not found (sid 2, awaited after the meta publish)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $stored = $client->jetStream()->objectStore('assets')->put('empty.txt', '')->await();

        self::assertSame(0, $stored->size);
        self::assertSame(0, $stored->chunks);

        $writes = implode('||', $transport->writes);
        // No chunk was published for the empty object; only the meta record.
        self::assertStringNotContainsString('PUB $O.assets.C.', $writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('empty.txt') . ' ', $writes);
    }

    /**
     * Verifies an overwrite purges the previous revision's chunk subject.
     */
    public function testPutOverwritePurgesPreviousChunks(): void
    {
        $oldNuid = 'oldnuid0001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // 1) existing-object lookup -> returns previous meta with old nuid
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // 2) chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // 3) meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
            // 4) purge old chunks ack
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->put('logo.txt', 'world')->await();

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * Verifies get downloads chunks via NUID subject and verifies the content digest.
     */
    public function testGetReturnsPayloadAndVerifiesDigest(): void
    {
        $nuid = 'nuidget0001';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),     // info() (Direct Get)
            $this->directChunkReply($nuid, 'hello', 2),   // single-chunk fast path (Direct Get on the NUID subject)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
        self::assertSame('doc.txt', $fetched->info->name);
        self::assertSame($nuid, $fetched->info->nuid);

        // The single chunk is fetched via Direct Get on its NUID subject (no ephemeral consumer).
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"last_by_subj":"$O.assets.C.' . $nuid . '"', $writes);
        self::assertStringNotContainsString('CONSUMER.CREATE', $writes);
    }

    public function testGetVerifiesUnpaddedBase64UrlDigest(): void
    {
        $nuid = 'nuidunpad01';
        // A non-Go client may store the digest as UNPADDED base64url; byte comparison must still
        // verify it against our padded computation instead of throwing a spurious mismatch.
        $unpadded = rtrim($this->digestOf('hello'), '=');
        self::assertNotSame($unpadded, $this->digestOf('hello')); // guard: the fixture is actually unpadded

        $meta = $this->metaGetResponse('doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $unpadded]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
    }

    /**
     * Verifies get throws on a content digest mismatch.
     */
    public function testGetThrowsOnDigestMismatch(): void
    {
        $nuid = 'nuidbad0001';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 11,
            'chunks' => 1,
            'digest' => $this->digestOf('hello world'),
        ]);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'CORRUPTED!!', 2), // body does not match the metadata digest
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies getToCallback streams the chunk payload to the handler.
     */
    public function testGetToCallbackStreamsChunks(): void
    {
        $nuid = 'nuidcb00001';
        $meta = $this->metaGetResponse('report.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $captured = '';
        $calls = 0;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'report.txt',
            static function (string $chunk) use (&$captured, &$calls): void {
                $captured .= $chunk;
                $calls++;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertSame(1, $calls);
        self::assertNotNull($info);
        self::assertSame('report.txt', $info->name);
    }

    /**
     * Verifies getToCallback invokes the callback once per stored chunk (in order) for a multi-chunk
     * object, rather than buffering the whole object and calling back once.
     */
    public function testGetToCallbackInvokesCallbackOncePerChunk(): void
    {
        $nuid = 'nuidcbmulti';
        $chunks = ['abc', 'def', 'ghi'];
        $assembled = implode('', $chunks);
        $meta = $this->metaGetResponse('multi.txt', [
            'nuid' => $nuid,
            'size' => strlen($assembled),
            'chunks' => count($chunks),
            'digest' => $this->digestOf($assembled),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHCBM","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            "MSG _INBOX.JS.FETCH.c 3 3\r\nabc\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\ndef\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\nghi\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $captured = [];
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'multi.txt',
            static function (string $chunk) use (&$captured): void {
                $captured[] = $chunk;
            },
        )->await();

        // One invocation per stored chunk, in order — the whole object is never assembled.
        self::assertSame(['abc', 'def', 'ghi'], $captured);
        self::assertNotNull($info);
        self::assertSame('multi.txt', $info->name);
        self::assertSame(3, $info->chunks);
    }

    /**
     * Verifies getToCallback returns null for a deleted object without invoking the callback.
     */
    public function testGetToCallbackReturnsNullForDeletedObjects(): void
    {
        $meta = $this->metaGetResponse('gone.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $called = false;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'gone.txt',
            static function (string $chunk) use (&$called): void {
                $called = true;
            },
        )->await();

        self::assertFalse($called);
        // A deleted object reads as not-available; the tombstone stays visible via info().
        self::assertNull($info);
    }

    /**
     * Verifies delete writes a tombstone via the encoded meta subject and purges chunks.
     */
    public function testDeleteWritesTombstoneAndPurgesChunks(): void
    {
        $oldNuid = 'delnuid0001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // The tombstone publish is issued before the concurrent lookup (no chunk to await first):
            // 1) tombstone meta publish ack (sid 1)
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),
            // 2) existing-object lookup -> previous revision (sid 2), awaited before the purge
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // 3) purge ack (sid 3)
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * Verifies list enumerates meta subjects and reads the latest record per object.
     */
    public function testListEnumeratesMetaSubjects(): void
    {
        $logoEnc = $this->encodeName('logo.txt');
        $oldEnc = $this->encodeName('old.txt');

        $streamInfo = json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => [
                'messages' => 3,
                'last_seq' => 3,
                'subjects' => [
                    '$O.assets.M.' . $logoEnc => 1,
                    '$O.assets.M.' . $oldEnc => 1,
                ],
            ],
        ], JSON_THROW_ON_ERROR);

        $logoExtra = ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')];
        $oldExtra = ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true];

        $emptyPage = '{"state":{"subjects":{}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // list(): paginated enumeration — page 1 (sid 1) returns the subjects, the empty page 2
            // (sid 2) terminates the loop — then a concurrent Direct Get per meta subject (logo ->
            // sid 3, old -> sid 4).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
            $this->directMetaReply('logo.txt', $logoExtra, 3),
            $this->directMetaReply('old.txt', $oldExtra, 4),
            // list(includeDeleted: true): same again — pages (sid 5, 6) then Direct Gets (sid 7, 8).
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.f 6 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
            $this->directMetaReply('logo.txt', $logoExtra, 7),
            $this->directMetaReply('old.txt', $oldExtra, 8),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $active = $bucket->list()->await();
        $all = $bucket->list(includeDeleted: true)->await();

        self::assertCount(1, $active);
        self::assertSame('logo.txt', $active[0]->name);
        self::assertCount(2, $all);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"subjects_filter":"$O.assets.M.>"', $writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.OBJ_assets', $writes);
    }

    public function testListPaginatesAcrossSubjectPages(): void
    {
        // The subjects map is server-capped, so a large bucket is enumerated across pages. Here each
        // object arrives on its own page; list() must collect both (not truncate to the first page).
        $page1 = (string) json_encode(['state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('a.txt') => 1]]], JSON_THROW_ON_ERROR);
        $page2 = (string) json_encode(['state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('b.txt') => 1]]], JSON_THROW_ON_ERROR);
        $emptyPage = '{"state":{"subjects":{}}}';
        $extra = ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')];

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page1), $page1),           // page 1 (sid 1): a.txt
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($page2), $page2),           // page 2 (sid 2): b.txt
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),   // page 3 (sid 3): empty -> stop
            $this->directMetaReply('a.txt', $extra, 4),
            $this->directMetaReply('b.txt', $extra, 5),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        // Both objects are returned (one came from page 2 — the old single-page code would miss it).
        self::assertCount(2, $objects);
        $names = array_map(static fn ($o): string => $o->name, $objects);
        sort($names);
        self::assertSame(['a.txt', 'b.txt'], $names);

        // Verify three STREAM.INFO pages were requested at increasing offsets.
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"offset":0', $writes);
        self::assertStringContainsString('"offset":1', $writes);
        self::assertStringContainsString('"offset":2', $writes);
    }

    public function testInfoFallsBackToStreamMessageWhenDirectGetUnavailable(): void
    {
        // Direct Get -> 503 no-responders (allow_direct disabled / interop bucket); info() must fall
        // back to the leader STREAM.MSG.GET path and still return the metadata.
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directStatusReply(1, 503, 'No Responders'),                       // Direct Get (sid 1)
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($meta), $meta),            // STREAM.MSG.GET fallback (sid 2)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.OBJ_assets', $writes);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.OBJ_assets', $writes);
    }

    /**
     * Verifies info returns null when JetStream reports the object metadata is not found.
     */
    public function testInfoReturnsNullWhenNotFound(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directStatusReply(1, 404, 'Message Not Found'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        self::assertNull($client->jetStream()->objectStore('assets')->info('missing.txt')->await());
    }

    /**
     * Verifies watch parses metadata payloads and dispatches ObjectInfo updates.
     */
    public function testWatchDispatchesObjectInfo(): void
    {
        $enc = $this->encodeName('logo.txt');
        $metadata = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'wnuid1',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], JSON_THROW_ON_ERROR);

        // watch() runs over a JetStream push consumer: create it (sid 1), then the update is delivered
        // on the deliver inbox (sid 2) carrying its stream sequence in the $JS.ACK reply -> revision.
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.1.7.1.0.0 %d\r\n%s\r\n", $enc, strlen((string) $metadata), (string) $metadata),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $sid = $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen = $info;
        })->await();

        self::assertSame(2, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(ObjectInfo::class, $seen);
        /** @var ObjectInfo $seenInfo */
        $seenInfo = $seen;
        self::assertSame('logo.txt', $seenInfo->name);
        self::assertSame('wnuid1', $seenInfo->nuid);
        // The revision is the delivery's stream sequence (from the $JS.ACK reply).
        self::assertSame(7, $seenInfo->revision);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"new"', $writes);
        self::assertStringContainsString('"ack_policy":"none"', $writes);
    }

    public function testWatchToleratesMalformedMetadataAndKeepsDispatching(): void
    {
        $enc = $this->encodeName('logo.txt');
        $valid = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'wnuid2',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ], JSON_THROW_ON_ERROR);

        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // A malformed (non-JSON) meta delivery (sid 2) must be skipped, not throw out of the loop.
            sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.1.5.1.0.0 %d\r\n%s\r\n", $enc, strlen('not-json'), 'not-json'),
            // A subsequent valid meta on the same subscription is still delivered.
            sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.2.6.2.0.0 %d\r\n%s\r\n", $enc, strlen((string) $valid), (string) $valid),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = [];
        $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen[] = $info->nuid;
        })->await();

        // Both frames are processed; the malformed one is silently skipped, the valid one delivered.
        $client->processIncoming()->await();
        $client->processIncoming()->await();

        self::assertSame(['wnuid2'], $seen);
    }

    public function testBucketSubjectHelpers(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        self::assertSame('OBJ_assets', $bucket->streamName());
        self::assertSame('$O.assets.C.', $bucket->chunkPrefix());
        self::assertSame('$O.assets.M.', $bucket->metaPrefix());
    }

    public function testPutRejectsEmptyName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->put('', 'x')->await();
    }

    public function testPutSplitsIntoMultipleChunksWithSmallChunkSize(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // existing-object lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // three chunk acks
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // meta ack
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);
        $stored = $bucket->put('multi.bin', 'abcdefghij')->await();

        self::assertSame(3, $stored->chunks);
        $writes = implode('', $transport->writes);
        self::assertSame(3, substr_count($writes, 'PUB $O.assets.C.' . $stored->nuid));
        self::assertStringContainsString('"chunks":3', $writes);
    }

    /**
     * Verifies a multi-chunk object is downloaded with a single batched pull (not one pull per
     * chunk), reassembled in order, and digest-verified.
     */
    public function testGetDownloadsMultipleChunksInSingleBatch(): void
    {
        $nuid = 'nuidbatch01';
        $chunks = ['abc', 'def', 'ghi'];
        $assembled = implode('', $chunks);
        $meta = $this->metaGetResponse('multi.bin', [
            'nuid' => $nuid,
            'size' => strlen($assembled),
            'chunks' => count($chunks),
            'digest' => $this->digestOf($assembled),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHB","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),                     // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),             // create ephemeral consumer
            // All three chunks arrive on the single fetch-batch inbox (sid 3), no per-chunk pull.
            "MSG _INBOX.JS.FETCH.c 3 3\r\nabc\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\ndef\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\nghi\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),  // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('multi.bin')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame($assembled, $fetched->data);
        self::assertSame(count($chunks), $fetched->info->chunks);
        self::assertSame($this->digestOf($assembled), $fetched->info->digest);

        $writes = implode('||', $transport->writes);
        // One batched pull request for all chunks (batch == chunk count), not three separate pulls.
        self::assertStringContainsString('"filter_subject":"$O.assets.C.' . $nuid . '"', $writes);
        self::assertSame(1, substr_count($writes, 'PUB $JS.API.CONSUMER.MSG.NEXT.OBJ_assets'));
        self::assertStringContainsString('"batch":3', $writes);
    }

    /**
     * Verifies list() rethrows a non-404 error raised while reading a meta record.
     */
    public function testListRethrowsNonNotFoundError(): void
    {
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('doc.txt') => 1]],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),  // metaSubjects() page 1 (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen('{"state":{"subjects":{}}}'), '{"state":{"subjects":{}}}'), // terminator page (sid 2)
            $this->directStatusReply(3, 500, 'boom'),                                 // Direct Get -> non-404 error
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('boom');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    /**
     * Verifies list() surfaces an error from the meta-subject enumeration request.
     */
    public function testListThrowsWhenSubjectEnumerationFails(): void
    {
        $error = '{"error":{"code":500,"description":"info failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),  // metaSubjects() -> error
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('info failed');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    /**
     * Verifies delete() still succeeds when the best-effort chunk purge fails.
     */
    public function testDeleteToleratesPurgeFailure(): void
    {
        $meta = $this->metaGetResponse('logo.txt', ['nuid' => 'delnuidfail', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')]);
        $purgeError = '{"error":{"code":500,"description":"purge failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),                       // existing-object lookup
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),  // tombstone ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($purgeError), $purgeError),            // purge -> error (swallowed)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
    }

    /**
     * Verifies list() skips a meta subject whose record is no longer present (404).
     */
    public function testListSkipsNotFoundSubject(): void
    {
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => [
                '$O.assets.M.' . $this->encodeName('gone.txt') => 1,
                '$O.assets.M.' . $this->encodeName('logo.txt') => 1,
            ]],
        ], JSON_THROW_ON_ERROR);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo), // metaSubjects() page 1 (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen('{"state":{"subjects":{}}}'), '{"state":{"subjects":{}}}'), // terminator page (sid 2)
            $this->directStatusReply(3, 404, 'message not found'),                    // gone.txt -> 404 (skipped)
            $this->directMetaReply('logo.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 4), // logo.txt -> present
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        self::assertCount(1, $objects);
        self::assertSame('logo.txt', $objects[0]->name);
    }

    /**
     * Verifies delete() proceeds when the previous-metadata lookup fails with a non-404 error.
     */
    public function testDeleteToleratesMissingPreviousMetadata(): void
    {
        $lookupError = '{"error":{"code":500,"description":"lookup failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),        // tombstone ack (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($lookupError), $lookupError),               // lookup -> 500 swallowed (sid 2)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
    }

    /**
     * Verifies delete() surfaces an error when the metadata (tombstone) publish is rejected.
     */
    public function testDeleteThrowsWhenMetadataPublishFails(): void
    {
        $publishError = '{"error":{"code":400,"description":"publish rejected"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($publishError), $publishError),            // tombstone publish -> error (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),    // lookup -> 404 (sid 2)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('publish rejected');
        $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();
    }

    /**
     * Verifies a pull timeout (408) during download stops the loop and fails the completeness gate
     * (fewer chunks received than the metadata declares), rather than returning a truncated object.
     */
    public function testGetStopsOnPullTimeoutAndFailsCompleteness(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuidto01', 'size' => 5, 'chunks' => 2, 'digest' => $this->digestOf('hello')]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHTO","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 408 Request Timeout\r\nStatus: 408\r\nDescription: Request Timeout\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),                          // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),                  // create consumer
            sprintf("HMSG _INBOX.JS.FETCH.c 3 %d %d\r\n%s\r\n", $hb, $hb, $status),                 // pull -> 408 (break)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),       // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Incomplete object download');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies the completeness gate is digest-independent: a truncated download of an object whose
     * metadata has no digest (e.g. written by another client) still fails instead of returning a
     * partial payload.
     */
    public function testGetFailsTruncatedDownloadEvenWithoutDigest(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuidnd01', 'size' => 6, 'chunks' => 2, 'digest' => '']);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHND","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 408 Request Timeout\r\nStatus: 408\r\nDescription: Request Timeout\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),                          // info() (chunks=2, no digest)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),                  // create consumer (sid 2)
            sprintf("HMSG _INBOX.JS.FETCH.c 3 %d %d\r\n%s\r\n", $hb, $hb, $status),                 // pull -> 408, 0 of 2 chunks
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),       // delete consumer (sid 4)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // No digest to verify against, but the chunk-count gate still rejects the short read.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Incomplete object download');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies a non-timeout pull error during download is propagated rather than swallowed.
     */
    public function testGetRethrowsNonTimeoutPullError(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuiderr01', 'size' => 5, 'chunks' => 2, 'digest' => $this->digestOf('hello')]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHERR","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->directReplyFromEnvelope($meta, 1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            sprintf("HMSG _INBOX.JS.FETCH.c 3 %d %d\r\n%s\r\n", $hb, $hb, $status),                 // pull -> 409 (rethrow)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('status 409');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }
}
