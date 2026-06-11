<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Connection\ConnectionStats;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FixedNonceSigner;
use IDCT\NATS\Tests\Support\FlakyTransport;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

final class NatsConnectionTest extends TestCase
{
    /**
     * Verifies a successful handshake transitions state to open and sends CONNECT/PING.
     */
    public function testConnectTransitionsToOpenAndSendsConnectAndPing(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            options: new NatsOptions(servers: ['nats://127.0.0.1:4222'], name: 'unit-test-client'),
            transport: $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame('S1', $serverInfo->serverId);
        self::assertSame('tcp://127.0.0.1:4222|5000', $transport->connectCalls[0]);
        self::assertCount(2, $transport->writes);
        self::assertStringStartsWith('CONNECT ', $transport->writes[0]);
        self::assertSame("PING\r\n", $transport->writes[1]);
    }

    /**
     * Verifies handshake accepts +OK and responds to server PING before final PONG.
     */
    public function testConnectHandlesOkAndPingBeforePong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "+OK\r\n",
            "PING\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame("PONG\r\n", $transport->writes[2]);
    }

    /**
     * Verifies the handshake reassembles an INFO frame fragmented across TCP segments (regression for
     * issue #2). A long INFO (carrying an `xkey`, NATS 2.10+) can be split mid-frame over a real
     * network; the tail segment, if parsed on its own, looks like a bogus control line (e.g.
     * `...VDGU3DPK"}`). The parser must buffer the partial frame and only act on the reassembled whole,
     * rather than parsing the raw tail and throwing "Unsupported control frame".
     */
    public function testConnectReassemblesFragmentedInfoFrame(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,'
            . '"max_payload":1048576,"headers":true,"xkey":"XKEYVDGU3DPKVDGU3DPKVDGU3DPKVDGU3DPK"}' . "\r\n";
        $split = intdiv(strlen($info), 2);

        $transport = new FakeTransport([
            substr($info, 0, $split),  // first segment: incomplete INFO — the parser must buffer it
            substr($info, $split),     // tail segment completes the frame
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame('S1', $serverInfo->serverId);
    }

    /**
     * Verifies that an unknown control-line op during handshake raises a ConnectionException
     * wrapping the parser's ProtocolException.
     *
     * When the parser receives a CRLF-terminated line it does not recognise, it throws a
     * ProtocolException which the connection layer wraps in a ConnectionException.
     */
    public function testConnectFailsOnUnknownControlLineDuringHandshake(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "UNKNOWN\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Unsupported control frame: UNKNOWN');

        try {
            $connection->connect()->await();
        } finally {
            self::assertSame(ConnectionState::Closed, $connection->state());
        }
    }

    /**
     * Verifies that an incomplete chunk without CRLF exhausts the poll budget and times out.
     *
     * When the transport delivers a partial line that never terminates, the parser buffers it
     * indefinitely and the handshake loop runs out of polls, resulting in a ConnectionException.
     */
    public function testConnectFailsWhenNoPongAndMovesToClosed(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            'INCOMPLETE_NO_CRLF',
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Expected PONG after CONNECT');

        try {
            $connection->connect()->await();
        } finally {
            self::assertSame(ConnectionState::Closed, $connection->state());
        }
    }

    /**
     * Verifies handshake fails fast when server emits an error line.
     */
    public function testConnectFailsOnServerErrLine(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "-ERR Maximum Connections Exceeded\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server error during connect');

        $connection->connect()->await();
    }

    /**
     * Verifies an authorization error during connect raises AuthenticationException and is NOT retried (#46).
     */
    public function testConnectAuthErrorThrowsAuthenticationExceptionWithoutRetry(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "-ERR Authorization Violation\r\n",
        ]);

        // Reconnect is ENABLED, yet an auth error must fail fast (a single connect attempt).
        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 5),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected AuthenticationException');
        } catch (\IDCT\NATS\Exception\AuthenticationException $e) {
            self::assertStringContainsString('authentication', strtolower($e->getMessage()));
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        // Exactly one connect attempt was made (CONNECT written once), not maxReconnectAttempts.
        self::assertSame(1, count(array_filter($transport->writes, static fn(string $w): bool => str_starts_with($w, 'CONNECT '))));
    }

    /**
     * Verifies JWT auth signs the server nonce from INFO in CONNECT payload.
     */
    public function testConnectIncludesJwtSignatureFromInfoNonce(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"nonce":"n-123"}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(jwt: 'jwt-token', nkey: 'UABC123', nonceSigner: new FixedNonceSigner('sig:')),
            $transport,
        );

        $connection->connect()->await();

        self::assertStringContainsString('"jwt":"jwt-token"', $transport->writes[0]);
        self::assertStringContainsString('"sig":"sig:n-123"', $transport->writes[0]);
        self::assertStringContainsString('"nkey":"UABC123"', $transport->writes[0]);
    }

    /**
     * Verifies publishing is rejected when connection is not open.
     */
    public function testPublishRequiresOpenConnection(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');

        $connection->publish('a.b', 'payload')->await();
    }

    /**
     * Verifies disconnect closes transport and updates state.
     */
    public function testDisconnectClosesTransportAndState(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $connection->disconnect()->await();

        self::assertTrue($transport->closed);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Verifies subscribe/unsubscribe commands are emitted with expected SID.
     */
    public function testSubscribeAndUnsubscribeSendProtocolCommands(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $sid = $connection->subscribe('orders.created', static function (NatsMessage $message): void {})->await();

        $connection->unsubscribe($sid)->await();

        self::assertSame(1, $sid);
        self::assertSame("SUB orders.created 1\r\n", $transport->writes[2]);
        self::assertSame("UNSUB 1\r\n", $transport->writes[3]);
    }

    /**
     * Verifies MSG frames are dispatched to matching subscription handlers.
     */
    public function testProcessIncomingDispatchesMsgToSubscriber(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame('updates', $receivedMessage->subject);
        self::assertSame('hello', $receivedMessage->payload);
    }

    /**
     * Verifies a delivered message can reply to its own reply subject via respond() (#17).
     */
    public function testDeliveredMessageCanRespondToReplySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 1 _INBOX.reply 4\r\nping\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $connection->subscribe('svc.echo', static function (NatsMessage $message): void {
            self::assertTrue($message->isReplyable());
            $message->respond('pong')->await();
        })->await();

        $connection->processIncoming()->await();

        $replies = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'PUB _INBOX.reply '),
        ));
        self::assertCount(1, $replies);
        self::assertSame("PUB _INBOX.reply 4\r\npong\r\n", $replies[0]);
    }

    /**
     * Verifies respond() with headers emits an HPUB to the reply subject (#17).
     */
    public function testDeliveredMessageCanRespondWithHeaders(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 1 _INBOX.reply 4\r\nping\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $connection->subscribe('svc.echo', static function (NatsMessage $message): void {
            $message->respondWithHeaders('pong', ['X-Trace' => 'abc'])->await();
        })->await();

        $connection->processIncoming()->await();

        $replies = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'HPUB _INBOX.reply '),
        ));
        self::assertCount(1, $replies);
        self::assertStringContainsString('X-Trace:abc', $replies[0]);
    }

    /**
     * Verifies respond() throws when the message carries no reply subject (#17).
     */
    public function testRespondThrowsWithoutReplySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $caught = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$caught): void {
            self::assertFalse($message->isReplyable());
            try {
                $message->respond('nope')->await();
            } catch (\LogicException $e) {
                $caught = $e;
            }
        })->await();

        $connection->processIncoming()->await();

        self::assertInstanceOf(\LogicException::class, $caught);
        self::assertStringContainsString('no reply subject', $caught->getMessage());
    }

    /**
     * Verifies a message constructed outside the delivery path cannot respond (#17).
     */
    public function testRespondThrowsWhenNotBoundToConnection(): void
    {
        $message = new NatsMessage('svc.echo', 1, '_INBOX.reply', 'ping');

        self::assertFalse($message->isReplyable());
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('not bound to a live connection');
        $message->respond('pong')->await();
    }

    /**
     * Verifies the connection listener receives Connected then Closed across the lifecycle (#22).
     */
    public function testConnectionListenerReceivesConnectedAndClosed(): void
    {
        $events = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(connectionListener: static function (ConnectionEvent $e, ?\Throwable $err) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );

        $connection->connect()->await();
        $connection->disconnect()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $events);
    }

    /**
     * Verifies an async INFO update emits LameDuck and DiscoveredServers events (#22).
     */
    public function testConnectionListenerReceivesLameDuckAndDiscoveredServers(): void
    {
        $events = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n",
        ]);

        $connection = new NatsConnection(
            // reconnectEnabled: false isolates event emission from the lame-duck auto-failover (#47).
            new NatsOptions(reconnectEnabled: false, connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );

        $connection->connect()->await();
        $connection->processIncoming()->await();

        // Discovery is applied before lame-duck (so a failover can use freshly-advertised peers).
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
        ], $events);
    }

    /**
     * Verifies the error listener is notified of slow-consumer drops (#23).
     */
    public function testErrorListenerReceivesSlowConsumerDrop(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 1\r\nA\r\nMSG updates 1 1\r\nB\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                maxPendingMessagesPerSubscription: 1,
                slowConsumerPolicy: SlowConsumerPolicy::DropOldest,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $connection->processIncoming()->await();

        self::assertCount(1, $errors);
        self::assertStringContainsString('Slow consumer', $errors[0]);
    }

    /**
     * Verifies the error listener is notified of recoverable server -ERR frames (#23).
     */
    public function testErrorListenerReceivesRecoverableServerError(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation for Subscription to foo'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(errorListener: static function (\Throwable $err) use (&$errors): void {
                $errors[] = $err->getMessage();
            }),
            $transport,
        );
        $connection->connect()->await();

        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(1, $errors);
        self::assertStringContainsString('recoverable error frame', $errors[0]);
    }

    /**
     * Verifies drainSubscription() UNSUBs, flushes, delivers the in-flight message, then drops the sub (#43).
     */
    public function testDrainSubscriptionDeliversInFlightThenRemoves(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // The drain flush reads this chunk: an in-flight message for sid 1, then the flush PONG.
            "MSG updates 1 4\r\nlast\r\nPONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        $connection->drainSubscription($sid)->await();

        self::assertSame(['last'], $delivered);
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('UNSUB ' . $sid . "\r\n", $writes);
        self::assertStringContainsString("PING\r\n", $writes);

        // The subscription is gone: a further frame for that sid is ignored.
        self::assertSame(0, $connection->processIncoming()->await());
    }

    /**
     * Verifies connection accessors + traffic statistics (#52).
     */
    public function testConnectionAccessorsAndStatistics(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG x 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(servers: ['nats://127.0.0.1:4222']), $transport);
        $connection->connect()->await();

        self::assertSame('nats://127.0.0.1:4222', $connection->connectedUrl());
        self::assertSame(1048576, $connection->maxPayload());

        $connection->subscribe('x', static function (NatsMessage $m): void {})->await();
        $connection->publish('a.b', 'hi')->await();
        $connection->processIncoming()->await();

        $stats = $connection->statistics();
        self::assertInstanceOf(ConnectionStats::class, $stats);
        self::assertSame(1, $stats->outMsgs);
        self::assertSame(2, $stats->outBytes);
        self::assertSame(1, $stats->inMsgs);
        self::assertSame(5, $stats->inBytes);

        $connection->disconnect()->await();
        self::assertNull($connection->connectedUrl());
    }

    /**
     * Verifies rtt() measures a PING/PONG round trip (#52).
     */
    public function testRttMeasuresPingPong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $rtt = $connection->rtt()->await();

        self::assertGreaterThanOrEqual(0.0, $rtt);
        self::assertLessThan(5.0, $rtt);
    }

    /**
     * Verifies discoveredServers() reflects connect_urls from an async INFO (#47).
     */
    public function testDiscoveredServersFromAsyncInfo(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"connect_urls":["10.0.0.2:4222","10.0.0.3:4222"]}' . "\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame([], $connection->discoveredServers());

        $connection->processIncoming()->await();

        self::assertSame(['10.0.0.2:4222', '10.0.0.3:4222'], $connection->discoveredServers());
    }

    /**
     * Verifies publishes during an in-flight reconnect are buffered and flushed on reconnect (#49).
     */
    public function testPublishBuffersDuringReconnectAndFlushesOnReconnect(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'], // connection 0: handshake, then EOF -> reconnect
                    [$info, "PONG\r\n"],            // connection 1: reconnect handshake
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    // Hold the reconnect (second connect) open so a publish lands mid-reconnect.
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        // Drive the read loop in the background: it reads EOF, starts reconnect, and blocks in connect().
        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05); // let the reconnect begin and suspend on the held connect()

        self::assertSame(ConnectionState::Connecting, $connection->state());
        // This publish lands during the reconnect window: it must buffer (not throw).
        $connection->publish('a.b', 'buffered')->await();

        $release->complete();   // let the reconnect finish
        $pump->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertContains("PUB a.b 8\r\nbuffered\r\n", $transport->writes);
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    /**
     * Verifies HMSG frames preserve raw headers and payload separation.
     */
    public function testProcessIncomingDispatchesHmsgWithRawHeaders(): void
    {
        $headerPayload = "NATS/1.0\r\n\r\n";
        $bodyPayload = 'hello';
        $merged = $headerPayload . $bodyPayload;

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG updates 1 12 17\r\n{$merged}\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $connection->processIncoming()->await();

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame($headerPayload, $receivedMessage->rawHeaders);
        self::assertSame($bodyPayload, $receivedMessage->payload);
    }

    /**
     * Verifies server PING frames are answered with protocol PONG.
     */
    public function testProcessIncomingRespondsToServerPing(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PING\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();
        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertSame("PONG\r\n", $transport->writes[2]);
    }

    /**
     * Verifies overflow with drop-oldest policy keeps newest buffered message.
     */
    public function testSlowConsumerDropOldestPolicy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::DropOldest,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $delivered = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['second'], $delivered);
    }

    /**
     * Verifies overflow with drop-newest policy keeps the earliest buffered message.
     */
    public function testSlowConsumerDropNewestPolicy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::DropNewest,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $delivered = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['first'], $delivered);
    }

    /**
     * Verifies overflow with error policy raises a connection exception.
     */
    public function testSlowConsumerErrorPolicyThrows(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Subscription queue overflow');

        $connection->processIncoming()->await();
    }

    /**
     * Verifies request/reply returns the first received response message.
     */
    public function testRequestReturnsFirstReplyMessage(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $response = $connection->request('svc.echo', '{"x":1}', 50)->await();

        self::assertSame('hello', $response->payload);
        self::assertStringStartsWith('SUB _INBOX.', $transport->writes[2]);
        self::assertStringStartsWith('PUB svc.echo _INBOX.', $transport->writes[3]);
        self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
    }

    public function testRequestReturnsReplyDeliveredOnSameTickAsTimeout(): void
    {
        // A reply delivered in the same processIncoming() call the deadline fires in must be returned,
        // not discarded as a spurious timeout. The transport holds the reply chunk for 50ms (past the
        // 10ms request deadline, ignoring the cancellation) to reproduce the completion-vs-timeout race.
        $transport = new FakeTransport(
            [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
                "MSG _INBOX.any 1 5\r\nhello\r\n",
            ],
            holdChunkContaining: 'hello',
            holdSeconds: 0.05,
        );

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $response = $connection->request('svc.echo', '{"x":1}', 10)->await();

        self::assertSame('hello', $response->payload);
    }

    /**
     * Verifies request/reply raises timeout when no response arrives before deadline.
     */
    public function testRequestTimesOutWithoutReply(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timed out');

        try {
            $connection->request('svc.echo', '{"x":1}', 5)->await();
        } finally {
            self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
        }
    }

    /**
     * Verifies request-many collects multiple replies and stops at maxResponses (#21).
     */
    public function testRequestManyCollectsUpToMaxResponses(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 1\r\nA\r\nMSG _INBOX.any 1 1\r\nB\r\nMSG _INBOX.any 1 1\r\nC\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $replies = $connection->requestMany('svc.scan', 'q', null, 3, 1000)->await();

        $payloads = array_map(static fn(NatsMessage $m): string => $m->payload, $replies);
        self::assertSame(['A', 'B', 'C'], $payloads);
        self::assertStringStartsWith('PUB svc.scan _INBOX.', $transport->writes[3]);
        self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
    }

    /**
     * Verifies request-many stops once the per-message stall interval elapses (#21).
     */
    public function testRequestManyStopsOnStallInterval(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 1\r\nA\r\nMSG _INBOX.any 1 1\r\nB\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        // No maxResponses: collection ends because no further reply arrives within the 20ms stall.
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 5000, 20)->await();

        self::assertCount(2, $replies);
    }

    /**
     * Verifies request-many returns an empty set on a no-responders sentinel (#21).
     */
    public function testRequestManyReturnsEmptyOnNoResponders(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'HMSG _INBOX.any 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $replies = $connection->requestMany('svc.scan', 'q', null, null, 1000)->await();

        self::assertSame([], $replies);
    }

    /**
     * Verifies draining stops cleanly if a handler unsubscribes itself mid-delivery.
     */
    public function testDrainStopsWhenHandlerUnsubscribesItself(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $sid = 0;
        $delivered = [];
        $sid = $connection->subscribe('updates', function (NatsMessage $message) use (&$delivered, &$connection, &$sid): void {
            $delivered[] = $message->payload;
            if ($message->payload === 'first') {
                $connection->unsubscribe($sid)->await();
            }
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['first'], $delivered);
    }

    /**
     * Verifies credentials embedded in the server URL are applied to CONNECT and stripped from the dial (#37).
     */
    public function testConnectExtractsUrlCredentials(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(servers: ['nats://alice:s3cret@127.0.0.1:4222']),
            $transport,
        );
        $connection->connect()->await();

        // The userinfo is stripped from the dialed DSN...
        self::assertSame('tcp://127.0.0.1:4222|5000', $transport->connectCalls[0]);
        // ...and applied to the CONNECT payload.
        $connect = $transport->writes[0];
        self::assertStringStartsWith('CONNECT ', $connect);
        self::assertStringContainsString('"user":"alice"', $connect);
        self::assertStringContainsString('"pass":"s3cret"', $connect);
    }

    /**
     * Verifies request uses configured inbox prefix for subscription and publish reply subject.
     */
    public function testRequestUsesConfiguredInboxPrefix(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG TMPBOX.any 1 5\r\nhello\r\n",
        ]);

        $options = new NatsOptions(inboxPrefix: 'TMPBOX');
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $connection->request('svc.echo', '{"x":1}', 50)->await();

        self::assertStringStartsWith('SUB TMPBOX.', $transport->writes[2]);
        self::assertStringStartsWith('PUB svc.echo TMPBOX.', $transport->writes[3]);
    }

    /**
     * Verifies request rejects non-positive timeout values.
     */
    public function testRequestRejectsNonPositiveTimeout(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timeout must be greater than zero');

        $connection->request('svc.echo', '{"x":1}', 0)->await();
    }

    /**
     * Verifies request propagates external cancellation and still unsubscribes inbox listener.
     */
    public function testRequestCanBeCancelledAndCleansUpSubscription(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $deferredCancellation = new DeferredCancellation();
        $deferredCancellation->cancel();

        $this->expectException(CancelledException::class);

        try {
            $connection->request('svc.echo', '{"x":1}', 1_000, $deferredCancellation->getCancellation())->await();
        } finally {
            self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
        }
    }

    /**
     * Verifies processIncoming reconnects after read failure and replays subscriptions.
     */
    public function testProcessIncomingReconnectsAndResubscribesAfterReadFailure(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(0, $connection->processIncoming()->await());
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(2, $transport->connectCalls);
        self::assertSame("SUB updates 1\r\n", $transport->writes[2]);
        self::assertSame("SUB updates 1\r\n", $transport->writes[5]);
    }

    public function testCallbackMayPublishDuringPostReconnectDeliveryWithoutDeadlock(): void
    {
        // A message buffered during reconnect replay is now delivered AFTER recovery exits its critical
        // section (not inside it). So a handler that publishes when invoked completes normally instead of
        // re-entering recoverConnection() and deadlocking on the in-progress reconnect.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n", // buffered during replay
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0, pingIntervalSeconds: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received, $connection): void {
            $received[] = $message->payload;
            // Publishing from within the post-reconnect delivery must not deadlock.
            $connection->publish('ack.' . $message->payload, 'ok')->await();
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertStringContainsString('PUB ack.hello 2', implode('', $transport->writes));
    }

    public function testProcessIncomingRecoversOnPeerEof(): void
    {
        // A graceful peer close (EOF), not a thrown read error, must still trigger reconnect + replay.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(0, $connection->processIncoming()->await()); // hits EOF -> recovers
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(2, $transport->connectCalls);
        self::assertSame("SUB updates 1\r\n", $transport->writes[5]);
    }

    public function testProcessIncomingRecoversOnPeerEofWithPingsDisabled(): void
    {
        // With pings disabled the read path is the ONLY way to detect loss, so EOF recovery is the
        // only thing standing between a peer close and a permanent silent outage.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0, pingIntervalSeconds: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        self::assertSame(0, $connection->processIncoming()->await());

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
    }

    public function testProcessIncomingMovesToClosedOnPeerEofWhenReconnectDisabled(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // recoverConnection() throws 'Reconnect is disabled'; the connection is left Closed.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $transport->connectCalls);
    }

    public function testConsumeHeartbeatResponseRecoversOnPeerEof(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        // The heartbeat self-read hits EOF: it must recover (after clearing readInProgress), not swallow.
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertCount(2, $transport->connectCalls);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testConsumeHeartbeatResponseDoesNotRecoverWithoutEof(): void
    {
        // An empty/no-PONG-yet read (not EOF) must be swallowed, never trigger a reconnect.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertCount(1, $transport->connectCalls);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies connect retries across rotated servers when earlier attempts fail.
     */
    public function testConnectRotatesServersOnReconnectAttempts(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S3","server_name":"n3","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 1,
            readFailures: 0,
        );

        $options = new NatsOptions(
            servers: ['nats://127.0.0.1:4222', 'nats://127.0.0.2:4222'],
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
        self::assertStringStartsWith('tcp://127.0.0.1:4222', $transport->connectCalls[0]);
        self::assertStringStartsWith('tcp://127.0.0.2:4222', $transport->connectCalls[1]);
    }

    /**
     * Verifies that receiving a server PONG frame does not throw and resets ping tracking.
     */
    public function testProcessIncomingHandlesServerPongSilently(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies async INFO frames refresh server capabilities during an open connection.
     */
    public function testProcessIncomingUpdatesServerInfoFromAsyncInfoFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":64,"headers":true}' . "\r\n",
            "PONG\r\n",
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.12.1\",\"jetstream\":true,\"max_payload\":128,\"headers\":true}\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame(64, $serverInfo->maxPayload);

        $frames = $connection->processIncoming()->await();

        $updatedServerInfo = $connection->serverInfo();
        self::assertNotNull($updatedServerInfo);
        self::assertSame(1, $frames);
        self::assertSame(128, $updatedServerInfo->maxPayload);
        self::assertSame('2.12.1', $updatedServerInfo->version);
    }

    /**
     * Verifies recoverable server permission errors do not close the connection.
     */
    public function testProcessIncomingIgnoresRecoverableServerErrFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation for Publish to updates'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies the ping timer sends PING frames at the configured interval.
     */
    public function testPingTimerSendsPingAtInterval(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        // Use a very short interval (1 second) and let the event loop tick.
        $options = new NatsOptions(
            pingIntervalSeconds: 1,
            maxPingsOut: 3,
            reconnectEnabled: false,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $writesBeforePing = count($transport->writes);

        // Let the event loop run long enough for the timer to fire.
        delay(1.1);

        self::assertGreaterThan($writesBeforePing, count($transport->writes));
        self::assertSame("PING\r\n", $transport->writes[$writesBeforePing]);

        $connection->disconnect()->await();
    }

    /**
     * Verifies ping timer is not started when pingIntervalSeconds is zero.
     */
    public function testPingTimerDisabledWhenIntervalIsZero(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $options = new NatsOptions(
            pingIntervalSeconds: 0,
            reconnectEnabled: false,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $writesAfterConnect = count($transport->writes);

        delay(0.1);

        // No additional writes (no PING sent).
        self::assertCount($writesAfterConnect, $transport->writes);

        $connection->disconnect()->await();
    }

    /**
     * Verifies disconnect cancels the ping timer cleanly.
     */
    public function testDisconnectCancelsPingTimer(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $options = new NatsOptions(
            pingIntervalSeconds: 1,
            maxPingsOut: 2,
            reconnectEnabled: false,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();
        $connection->disconnect()->await();

        $writesAfterDisconnect = count($transport->writes);

        // Let event loop tick past when timer would have fired.
        delay(1.2);

        // No PING sent after disconnect.
        self::assertCount($writesAfterDisconnect, $transport->writes);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Verifies ping timer marks connection closed when max outstanding pings is exceeded and reconnect fails.
     */
    public function testPingTimerClosesWhenMaxPingsExceededAndReconnectFails(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 1,
                maxPingsOut: 0,
                reconnectEnabled: false,
            ),
            $transport,
        );
        $connection->connect()->await();

        delay(1.1);

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Verifies publish throws when payload exceeds server max_payload.
     */
    public function testPublishRejectsOversizedPayload(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":64,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Payload size 65 exceeds server max_payload of 64');

        $connection->publish('test.subject', str_repeat('x', 65))->await();
    }

    /**
     * Verifies publish succeeds when payload exactly matches max_payload.
     */
    public function testPublishAcceptsPayloadAtExactLimit(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":64,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $connection->publish('test.subject', str_repeat('x', 64))->await();

        self::assertStringContainsString('PUB test.subject 64', $transport->writes[2]);
    }

    /**
     * Verifies publishWithHeaders checks total (headers + payload) against max_payload.
     */
    public function testPublishWithHeadersRejectsOversizedTotal(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":32,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('exceeds server max_payload of 32');

        $connection->publishWithHeaders('test.subject', str_repeat('x', 32), ['Key' => 'Val'])->await();
    }

    /**
     * Verifies CONNECT payload includes no_responders flag.
     */
    public function testConnectPayloadIncludesNoResponders(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertStringContainsString('"no_responders":true', $transport->writes[0]);
    }

    /**
     * Verifies request throws NatsException when server returns 503 No Responders.
     */
    public function testRequestThrowsOnNoRespondersStatus(): void
    {
        $noRespondersHeader = "NATS/1.0 503 No Responders\r\n\r\n";
        $headerBytes = strlen($noRespondersHeader);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG _INBOX.any 1 {$headerBytes} {$headerBytes}\r\n{$noRespondersHeader}\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('No responders for subject svc.missing');

        $connection->request('svc.missing', 'hello', 500)->await();
    }

    // ─── Subject Validation ─────────────────────────────────────────────

    public function testPublishRejectsEmptySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not be empty');
        $connection->publish('', 'data')->await();
    }

    public function testPublishRejectsSubjectWithWhitespace(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain whitespace');
        $connection->publish('foo bar', 'data')->await();
    }

    public function testPublishRejectsWildcardSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards are not allowed in publish subjects');
        $connection->publish('foo.*', 'data')->await();
    }

    public function testPublishRejectsEmptyTokenInSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain empty tokens');
        $connection->publish('foo..bar', 'data')->await();
    }

    public function testPublishRejectsFullWildcardToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards are not allowed in publish subjects');
        $connection->publish('foo.>', 'data')->await();
    }

    public function testSubscribeAcceptsWildcardSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $sid = $connection->subscribe('foo.*', function (): void {})->await();
        self::assertSame(1, $sid);

        $sid2 = $connection->subscribe('bar.>', function (): void {})->await();
        self::assertSame(2, $sid2);
    }

    public function testSubscribeRejectsGreaterThanNotInLastToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcard ">" must be the last token');
        $connection->subscribe('>.foo', function (): void {})->await();
    }

    public function testSubscribeRejectsPartialWildcardToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards must occupy an entire token');
        $connection->subscribe('foo.ba*', function (): void {})->await();
    }

    // ─── Drain ──────────────────────────────────────────────────────────

    public function testDrainUnsubscribesAllAndCloses(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $connection->subscribe('foo', function (): void {})->await();
        $connection->subscribe('bar', function (): void {})->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertTrue($transport->closed);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
        self::assertStringContainsString("UNSUB 2\r\n", $writes);
        self::assertStringContainsString("PING\r\n", $writes);
    }

    public function testSubscriptionDispatchIsNotReentrantWhenHandlerAwaits(): void
    {
        // First message's handler awaits a request() on the same connection. That request self-pumps
        // processIncoming(), which reads the SECOND message for the same sid and tries to drain it.
        // Without the per-sid re-entrancy guard the second handler would fire on top of the suspended
        // first one (log: start:A, start:B, end:B, end:A); with it, B waits for A to finish.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG events 1 1\r\nA\r\n",       // first delivery (sid 1)
            "MSG events 1 1\r\nB\r\n",       // second delivery (sid 1), read during the awaited request
            "MSG _INBOX.r 2 1\r\nR\r\n",     // the awaited request's reply (inbox sid 2)
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $log = [];
        $first = true;
        $connection->subscribe('events', function (NatsMessage $message) use (&$log, &$first, $connection): void {
            $tag = $message->payload;
            $log[] = 'start:' . $tag;
            if ($first) {
                $first = false;
                $connection->request('svc', 'x', 1000)->await();
            }
            $log[] = 'end:' . $tag;
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['start:A', 'end:A', 'start:B', 'end:B'], $log);
    }

    public function testFlushSendsPingAndResolvesOnPong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",   // connect handshake PONG
            "PONG\r\n",   // the flush() PONG
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $connection->flush()->await();

        // flush() wrote a PING and resolved once the server's PONG round-tripped.
        self::assertContains("PING\r\n", $transport->writes);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testDrainedSubscriptionQueuesAreNotRetained(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG events 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $connection->subscribe('events', function (): void {})->await();
        $connection->processIncoming()->await();

        // Once delivered, the per-SID pending queue is removed rather than retained as an empty queue,
        // so the per-chunk drain scan stays proportional to subscriptions with pending messages.
        $pending = (new \ReflectionProperty(NatsConnection::class, 'pendingMessages'))->getValue($connection);
        self::assertSame([], $pending);
    }

    public function testDrainDoesNotResurrectConnectionOnReadFailure(): void
    {
        // Connect, then the flush read FAILS (peer close / EOF) instead of returning the drain PONG.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            FakeTransport::EOF,
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 200, reconnectEnabled: true),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('events', function (): void {})->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());

        // A resurrection would reconnect (a second CONNECT) and re-SUBscribe the just-UNSUBbed sid.
        $writes = implode('', $transport->writes);
        self::assertSame(1, substr_count($writes, 'CONNECT '), 'drain must not reconnect on a read failure while draining');
        self::assertSame(1, substr_count($writes, 'SUB events '), 'drain must not re-subscribe while draining');
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
    }

    public function testDrainTerminatesViaDeadlineWhenNoFlushPongArrives(): void
    {
        // The queue empties with NO drain PONG: readLine returns '' synchronously. drain() must end
        // via its flush deadline rather than busy-spin (a synchronous 0-frame loop would starve the
        // event loop so the TimeoutCancellation could never fire). The fix yields between empty reads.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 150),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('events', function (): void {})->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testDrainRequiresOpenConnection(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->drain()->await();
    }

    public function testDrainDeliversBufferedMessagesBeforeClosing(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG events 1 5\r\nhello\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('events', static function (NatsMessage $msg) use (&$received): void {
            $received[] = $msg->payload;
        })->await();

        // Drain should flush the in-flight delivery, then close.
        $connection->drain()->await();

        self::assertSame(['hello'], $received);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testRequestTimeoutPreservesOriginalExceptionDuringCleanup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        try {
            $connection->request('svc.echo', '{"x":1}', 5)->await();
            self::fail('Expected timeout');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('Request timed out', $e->getMessage());
        }

        self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
    }

    public function testMalformedHmsgTriggersRecoveryInsteadOfEscaping(): void
    {
        // headerBytes (20) > totalBytes (10): a corrupt frame. The parser rejects it, and
        // processIncoming treats an unparseable stream as a transport failure (recovery) rather
        // than letting the ProtocolException escape the caller's loop. With reconnect disabled that
        // surfaces as a ConnectionException and closes the connection.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG updates 1 20 10\r\n1234567890\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, reconnectEnabled: false),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        try {
            $connection->processIncoming()->await();
            self::fail('Expected a ConnectionException from the corrupt-stream recovery path');
        } catch (ConnectionException) {
            // Expected: corrupt stream -> recoverConnection() -> reconnect disabled -> Closed.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    // ─── Exponential Backoff ────────────────────────────────────────────

    public function testBackoffDelayIsExponential(): void
    {
        // Use reflection to test private backoffDelayMs method.
        $transport = new FakeTransport();
        $connection = new NatsConnection(
            new NatsOptions(reconnectDelayMs: 100, reconnectMaxDelayMs: 5000, reconnectJitterMs: 0),
            $transport,
        );

        $method = new \ReflectionMethod($connection, 'backoffDelayMs');

        self::assertSame(100, $method->invoke($connection, 1));   // 100 * 2^0 = 100
        self::assertSame(200, $method->invoke($connection, 2));   // 100 * 2^1 = 200
        self::assertSame(400, $method->invoke($connection, 3));   // 100 * 2^2 = 400
        self::assertSame(800, $method->invoke($connection, 4));   // 100 * 2^3 = 800
        self::assertSame(1600, $method->invoke($connection, 5));  // 100 * 2^4 = 1600
        self::assertSame(3200, $method->invoke($connection, 6));  // 100 * 2^5 = 3200
        self::assertSame(5000, $method->invoke($connection, 7));  // 100 * 2^6 = 6400 → capped at 5000
        self::assertSame(5000, $method->invoke($connection, 10)); // capped
    }

    /**
     * Verifies requestWithHeaders uses HPUB and returns first reply message.
     */
    public function testRequestWithHeadersReturnsReply(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 2\r\nok\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $reply = $connection->requestWithHeaders('svc.echo', 'hi', ['X-Test' => '1'], 100)->await();

        self::assertSame('ok', $reply->payload);
        self::assertStringStartsWith('HPUB svc.echo _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('X-Test:1', $transport->writes[3]);
    }

    public function testProcessIncomingRequiresOpenConnection(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->processIncoming()->await();
    }

    public function testUnsubscribeRequiresOpenConnection(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->unsubscribe(1)->await();
    }

    public function testPublishWithHeadersRequiresOpenConnection(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->publishWithHeaders('orders.created', '{}', ['X' => '1'])->await();
    }

    public function testProcessIncomingThrowsOnErrFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation'\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server sent error frame');
        $connection->processIncoming()->await();
    }

    public function testConnectUsesDefaultServerWhenListEmpty(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(servers: []), $transport);
        $connection->connect()->await();

        self::assertSame('tcp://127.0.0.1:4222|5000', $transport->connectCalls[0]);
    }

    public function testSubscribeRejectsEmbeddedWildcardToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards must occupy an entire token');
        $connection->subscribe('orders.a*', static function (NatsMessage $message): void {})->await();
    }

    public function testPublishRecoversAndRetriesAfterWriteFailure(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connected = 0;
            private bool $failNextPub = true;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    if ($this->failNextPub && str_starts_with($bytes, 'PUB orders.created ')) {
                        $this->failNextPub = false;
                        throw new \RuntimeException('write failed');
                    }

                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $connection->publish('orders.created', '{"id":1}')->await();

        self::assertCount(2, $transport->connectCalls);
        self::assertStringContainsString('PUB orders.created ', implode('', $transport->writes));
    }

    public function testPublishWithHeadersRecoversAndRetriesAfterWriteFailure(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connected = 0;
            private bool $failNextHpub = true;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    if ($this->failNextHpub && str_starts_with($bytes, 'HPUB orders.created ')) {
                        $this->failNextHpub = false;
                        throw new \RuntimeException('write failed');
                    }

                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $connection->publishWithHeaders('orders.created', '{"id":1}', ['X-Test' => '1'])->await();

        self::assertCount(2, $transport->connectCalls);
        self::assertStringContainsString('HPUB orders.created ', implode('', $transport->writes));
    }

    public function testPingTimerReconnectsWhenMaxOutstandingPingsExceeded(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connected = 0;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 1,
                maxPingsOut: 0,
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        delay(1.1);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);

        $connection->disconnect()->await();
    }

    public function testReconnectRetriesWhenResubscribeGetsFatalServerError(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "-ERR 'Authorization Violation'\r\n",
                ],
                [
                    'INFO {"server_id":"S3","server_name":"n3","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(0, $connection->processIncoming()->await());
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(3, $transport->connectCalls);

        $subWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $write): bool => str_starts_with($write, 'SUB updates 1'),
        ));
        self::assertCount(3, $subWrites);
        self::assertSame('S3', $connection->serverInfo()?->serverId);
    }

    public function testPingTimerWriteFailureReconnectsWhenEnabled(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];

            private int $connected = 0;
            private bool $failFirstPing = true;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    if ($this->failFirstPing && $bytes === "PING\r\n") {
                        $this->failFirstPing = false;
                        throw new \RuntimeException('ping write failed');
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 1,
                maxPingsOut: 3,
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        delay(1.1);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);

        $connection->disconnect()->await();
    }

    /**
     * Verifies handshake succeeds when the INFO frame arrives in multiple TCP segments.
     *
     * Simulates a NATS 2.10+ server whose INFO frame (containing the xkey field) is large
     * enough to be split by TCP into two reads. The first read delivers a partial JSON payload
     * with no CRLF terminator; the second read delivers the remainder with the terminator.
     * The ProtocolParser must buffer the first chunk and complete the frame on the second read.
     */
    public function testConnectHandlesFragmentedInfoFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true,"xkey":"XNEYS5JGCB',
            "ID6OKSWDBK6DRYIBYQX3NWJQQSXMVDGU3DPK\"}\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies handshake succeeds when a re-INFO frame during the PONG phase is complete.
     *
     * This is the non-fragmented counterpart to the fragmented re-INFO regression test and
     * ensures awaitInitialPong() still handles a normal server INFO update before the final
     * PONG arrives.
     */
    public function testConnectHandlesNonFragmentedReInfoDuringPongPhase(): void
    {
        $transport = new FakeTransport([
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.10.0\",\"jetstream\":true,\"max_payload\":1048576,\"headers\":true}\r\n",
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.10.1\",\"jetstream\":true,\"max_payload\":2097152,\"headers\":true,\"xkey\":\"XNEYS5JGCBID6OKSWDBK6DRYIBYQX3NWJQQSXMVDGU3DPK\"}\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(2097152, $connection->serverInfo()?->maxPayload);
    }

    /**
     * Verifies handshake succeeds when a re-INFO frame during the PONG phase is fragmented.
     *
     * After the initial INFO exchange the server may send a second INFO (e.g. after TLS upgrade)
     * during the PONG phase. If that re-INFO contains the xkey field it can also be split across
     * TCP segments. The partial chunk must be buffered by the ProtocolParser and not parsed
     * directly via the raw-chunk fallback, which would receive truncated JSON and fail.
     */
    public function testConnectHandlesFragmentedReInfoDuringPongPhase(): void
    {
        $transport = new FakeTransport([
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.10.0\",\"jetstream\":true,\"max_payload\":1048576,\"headers\":true}\r\n",
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true,"xkey":"XNEYS5JGCB',
            "ID6OKSWDBK6DRYIBYQX3NWJQQSXMVDGU3DPK\"}\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    // ─── Reply / queue-group injection guards (P0-3) ────────────────────

    public function testPublishRejectsReplyToWithCrlfInjection(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain whitespace');
        $connection->publish('orders.created', 'data', "reply\r\nPUB hack 0\r\n")->await();
    }

    public function testPublishWithHeadersRejectsReplyToWithCrlfInjection(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain whitespace');
        $connection->publishWithHeaders('orders.created', 'data', ['X' => '1'], "reply\r\nPUB hack 0\r\n")->await();
    }

    public function testPublishAcceptsValidReplyTo(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $connection->publish('orders.created', 'data', '_INBOX.reply.1')->await();

        self::assertSame("PUB orders.created _INBOX.reply.1 4\r\ndata\r\n", $transport->writes[2]);
    }

    public function testSubscribeRejectsQueueGroupWithWhitespace(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Queue group must not contain whitespace');
        $connection->subscribe('tasks.process', static function (NatsMessage $message): void {}, "workers\r\nSUB hack 99")->await();
    }

    public function testSubscribeRejectsEmptyQueueGroup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Queue group must not be empty');
        $connection->subscribe('tasks.process', static function (NatsMessage $message): void {}, '')->await();
    }

    // ─── Cancellable reads / no orphaned read on timeout (P0-1) ─────────

    public function testRequestTimeoutCancelsReadAndAllowsSubsequentRequest(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            public int $maxConcurrentReads = 0;
            public int $cancelledReads = 0;
            private int $activeReads = 0;
            /** @var list<string> */
            public array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function () use ($cancellation): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    // No data: behave like a real, cancellable long-blocking socket read.
                    $this->activeReads++;
                    $this->maxConcurrentReads = max($this->maxConcurrentReads, $this->activeReads);

                    try {
                        delay(30, cancellation: $cancellation ?? new \Amp\NullCancellation());

                        return '';
                    } catch (\Amp\CancelledException $e) {
                        $this->cancelledReads++;

                        throw $e;
                    } finally {
                        $this->activeReads--;
                    }
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function pushReply(string $chunk): void
            {
                $this->queue[] = $chunk;
            }
        };

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // First request times out: the underlying read must be cancelled, not orphaned.
        try {
            $connection->request('svc.echo', 'x', 20)->await();
            self::fail('Expected timeout');
        } catch (TimeoutException) {
            // expected
        }

        self::assertGreaterThanOrEqual(1, $transport->cancelledReads);

        // A subsequent request must succeed with no reconnect and no overlapping read.
        $transport->pushReply("MSG _INBOX.any 2 2\r\nok\r\n");

        $reply = $connection->request('svc.echo', 'x', 500)->await();

        self::assertSame('ok', $reply->payload);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertLessThanOrEqual(1, $transport->maxConcurrentReads);
    }

    // ─── Idle heartbeat does not self-disconnect (P0-2) ─────────────────

    public function testIdleConnectionStaysOpenViaHeartbeatSelfRead(): void
    {
        // Live-server fake: every PING write produces a PONG on the next read. The application
        // never calls processIncoming(), so without the heartbeat self-read the outstanding-ping
        // counter would exceed maxPingsOut and (with reconnect disabled) close the connection.
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            public int $pings = 0;
            /** @var list<string> */
            private array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    if ($bytes === "PING\r\n") {
                        $this->pings++;
                        $this->queue[] = "PONG\r\n";
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function () use ($cancellation): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    delay(30, cancellation: $cancellation ?? new \Amp\NullCancellation());

                    return '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 1, maxPingsOut: 1, reconnectEnabled: false),
            $transport,
        );
        $connection->connect()->await();

        // Let several heartbeat ticks elapse without any application processIncoming() calls.
        delay(2.5);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertGreaterThanOrEqual(2, $transport->pings);

        $connection->disconnect()->await();
    }

    /**
     * Verifies a message captured during the heartbeat self-read is delivered to its subscription
     * immediately (via drainAllPending) rather than left buffered until the next processIncoming().
     */
    public function testHeartbeatResponseDeliversBufferedMessageImmediately(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            /** @var list<string> */
            private array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    if ($bytes === "PING\r\n") {
                        $this->queue[] = "PONG\r\n";
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function () use ($cancellation): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    delay(30, cancellation: $cancellation ?? new \Amp\NullCancellation());

                    return '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function enqueue(string $chunk): void
            {
                $this->queue[] = $chunk;
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 30, maxPingsOut: 5, reconnectEnabled: false),
            $transport,
        );
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('events', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        // Stage a delivery that the heartbeat self-read will pick up, then run that read directly.
        $transport->enqueue("MSG events 1 7\r\nupdated\r\n");
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        // Without drainAllPending() in consumeHeartbeatResponse() this would still be null.
        self::assertInstanceOf(NatsMessage::class, $received);
        self::assertSame('events', $received->subject);
        self::assertSame('updated', $received->payload);

        $connection->disconnect()->await();
    }

    public function testProcessIncomingSkipsWhenAnotherReadIsInProgress(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        // Simulate a read already owning the socket (e.g. the heartbeat self-read).
        (new \ReflectionProperty($connection, 'readInProgress'))->setValue($connection, true);

        // processIncoming() must not start a second overlapping read; it reports zero frames.
        self::assertSame(0, $connection->processIncoming()->await());
    }

    public function testHeartbeatReadSkippedWhenAnotherReadIsInProgress(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        (new \ReflectionProperty($connection, 'readInProgress'))->setValue($connection, true);

        // The heartbeat self-read must yield to the in-flight read rather than colliding with it.
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testHeartbeatReadHandlesEmptyErrorAndFatalFrames(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            public string $mode = 'empty';
            /** @var list<string> */
            private array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    if ($bytes === "PING\r\n") {
                        $this->queue[] = "PONG\r\n";
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    return match ($this->mode) {
                        'throw' => throw new \RuntimeException('transient read error'),
                        'fatal' => "-ERR 'fatal boom'\r\n",
                        default => '',
                    };
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        $invoke = new \ReflectionMethod($connection, 'consumeHeartbeatResponse');

        $transport->mode = 'empty';
        $invoke->invoke($connection); // empty read returns early

        $transport->mode = 'throw';
        $invoke->invoke($connection); // a transient read error is swallowed

        $transport->mode = 'fatal';
        $invoke->invoke($connection); // a fatal -ERR frame is swallowed rather than thrown out of the timer

        // None of these escalate out of the heartbeat read or close the connection.
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->disconnect()->await();
    }

    public function testProcessIncomingResetsPingCounterOnlyOnPong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $pings = new \ReflectionProperty($connection, 'outstandingPings');
        $pings->setValue($connection, 2);

        // A data frame is inbound traffic but NOT a PONG: it must not reset the watchdog counter
        // (otherwise a server that stops answering PINGs but still sends data is never detected).
        $connection->processIncoming()->await();
        self::assertSame(2, $pings->getValue($connection));

        // Only an actual PONG resets it.
        $connection->processIncoming()->await();
        self::assertSame(0, $pings->getValue($connection));
    }

    public function testDrainContinuesPastTransientEmptyReadUntilPong(): void
    {
        $received = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            '', // a transient 0-frame read during the flush must NOT end it early ...
            "MSG events 1 3\r\nabc\r\n", // ... so this server-flushed delivery is still delivered ...
            "PONG\r\n", // ... and the flush ends only on the PONG.
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $connection->subscribe('events', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $connection->drain()->await();

        self::assertContains('abc', $received);
    }

    // ─── TLS upgrade ordering (P1-4) ────────────────────────────────────

    public function testStandardTlsUpgradeRunsAfterInfoWhenNotHandshakeFirst(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The transport has TLS materials, so upgradeTls() actually establishes TLS.
        $transport->canUpgrade = true;

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: true, tlsHandshakeFirst: false, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // Post-INFO upgrade path performs exactly one explicit TLS upgrade and TLS is now active.
        self::assertSame(1, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
    }

    public function testServerRequiresTlsButNoMaterialsFailsBeforeWritingConnect(): void
    {
        // Server advertises tls_required; the client did NOT configure TLS (tlsRequired:false). The
        // client must fail fast and must NOT write CONNECT/PING (credentials) over a plaintext socket.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->canUpgrade = false; // no TLS materials -> upgradeTls cannot establish TLS

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: false, reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected a TLS ConnectionException');
        } catch (ConnectionException $e) {
            self::assertStringContainsStringIgnoringCase('tls', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('CONNECT ', $writes);
        self::assertStringNotContainsString('PING', $writes);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testServerRequiresTlsUpgradesThenSendsConnect(): void
    {
        // Server advertises tls_required and the transport has TLS materials: it upgrades, then the
        // CONNECT is written only after TLS is active.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->canUpgrade = true;

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: false, reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(1, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertStringContainsString('CONNECT ', implode('', $transport->writes));
    }

    public function testHandshakeFirstDoesNotCallExplicitUpgrade(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->tlsActiveOnConnect = true; // handshake-first established TLS during connect()

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: true, tlsHandshakeFirst: true, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // Handshake-first negotiates TLS during connect(), so no explicit post-INFO upgrade.
        self::assertSame(0, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
    }

    public function testHandshakeFirstWithoutEstablishedTlsFailsBeforeWritingConnect(): void
    {
        // Misconfiguration: tlsHandshakeFirst=true but no TLS materials/scheme (so the transport stays
        // plaintext), while the server's INFO advertises tls_required. The credentials fail-safe must
        // still fire on the handshake-first path and must NOT write CONNECT over the plaintext socket.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->tlsActiveOnConnect = false; // handshake-first did NOT establish TLS

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: true, reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected a TLS ConnectionException');
        } catch (ConnectionException $e) {
            self::assertStringContainsStringIgnoringCase('tls', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('CONNECT ', $writes);
        self::assertStringNotContainsString('PING', $writes);
        // Handshake-first never calls the explicit post-INFO upgrade.
        self::assertSame(0, $transport->upgradeTlsCalls);
    }

    public function testPlainConnectionDoesNotUpgradeTls(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(0, $transport->upgradeTlsCalls);
    }
}
