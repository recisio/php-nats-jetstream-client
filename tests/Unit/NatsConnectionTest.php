<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
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
            "-ERR Authentication Violation\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server error during connect');

        $connection->connect()->await();
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

    public function testMalformedHmsgRejectsInvalidHeaderBoundary(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG updates 1 20 10\r\n1234567890\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Malformed HMSG frame');

        $connection->processIncoming()->await();
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

    // ─── TLS upgrade ordering (P1-4) ────────────────────────────────────

    public function testStandardTlsUpgradeRunsAfterInfoWhenNotHandshakeFirst(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: true, tlsHandshakeFirst: false, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // Post-INFO upgrade path performs exactly one explicit TLS upgrade.
        self::assertSame(1, $transport->upgradeTlsCalls);
    }

    public function testHandshakeFirstDoesNotCallExplicitUpgrade(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: true, tlsHandshakeFirst: true, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // Handshake-first negotiates TLS during connect(), so no explicit post-INFO upgrade.
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
