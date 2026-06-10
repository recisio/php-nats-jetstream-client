<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class PullConsumerIteratorTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function jsMsg(string $subject, string $payload, string $replyTo): string
    {
        return sprintf("MSG %s 1 %s %d\r\n%s\r\n", $subject, $replyTo, strlen($payload), $payload);
    }

    public function testFluentBuilderSetsProperties(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $iter = $js->pullConsumer('ORDERS', 'PROC')
            ->setBatching(10)
            ->setExpiresMs(5000)
            ->setIterations(3);

        self::assertInstanceOf(PullConsumerIterator::class, $iter);
        self::assertSame(10, $iter->getBatching());
        self::assertSame(5000, $iter->getExpiresMs());
        self::assertSame(3, $iter->getIterations());
    }

    public function testDefaultValues(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        self::assertSame(1, $iter->getBatching());
        self::assertSame(3000, $iter->getExpiresMs());
        self::assertNull($iter->getIterations());
    }

    public function testSetBatchingRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setBatching(0);
    }

    public function testSetExpiresMsRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setExpiresMs(0);
    }

    public function testSetIterationsRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setIterations(0);
    }

    public function testSetIterationsAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')
            ->setIterations(5)
            ->setIterations(null);

        self::assertNull($iter->getIterations());
    }

    public function testHandleProcessesOneIteration(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // First iteration: 1 message delivered.
            $this->jsMsg('_INBOX.JS.FETCH.any', 'order-1', '$JS.ACK.ORDERS.PROC.1.1.1.123.0'),
            // Second iteration: terminal 404 status → JetStreamException breaks loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        self::assertSame(1, $total);
        self::assertSame(['order-1'], $processed);
    }

    public function testHandleStopsOnNoMessages(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // Immediately returns terminal 404 status — no messages.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $total = $client->jetStream()
            ->pullConsumer('STREAM', 'CONS')
            ->setBatching(5)
            ->setIterations(10)
            ->setExpiresMs(100)
            ->handle(function (): void {
                self::fail('Handler should not be called when no messages are available');
            })->await();

        self::assertSame(0, $total);
    }

    public function testHandleInfiniteModeContinuesPastEmptyWindow(): void
    {
        $noMessages = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";
        $h404 = strlen($noMessages);
        $h409 = strlen($deleted);
        $body = 'order-1';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): empty window (404) — infinite mode must keep polling, not stop.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $h404, $h404, $noMessages),
            // iter 2 (sid 2): a message arrives after the idle gap.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 3 (sid 3): a terminal error (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $h409, $h409, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // The message after the empty window is delivered (old code stopped on the first 404).
        self::assertSame(1, $total);
        self::assertSame(['order-1'], $processed);
    }

    public function testHandleInfiniteModeContinuesPastTransient409(): void
    {
        // A 409 can be transient (backpressure/failover/shutdown) or terminal (Consumer Deleted).
        // The status-line reason flows into the exception message via NatsHeaders::fromWireBlock.
        $maxAck = "NATS/1.0 409 Exceeded MaxAckPending\r\nStatus: 409\r\n\r\n"; // transient -> keep polling
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";      // terminal  -> stop
        $hMax = strlen($maxAck);
        $hDel = strlen($deleted);
        $body = 'job-7';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): transient 409 (backpressure) — infinite mode must keep polling.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hMax, $hMax, $maxAck),
            // iter 2 (sid 2): a message arrives once backpressure clears.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 3 (sid 3): a terminal 409 (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $hDel, $hDel, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // Old code treated the transient 409 as terminal and stopped before the message.
        self::assertSame(1, $total);
        self::assertSame(['job-7'], $processed);
    }

    /**
     * Verifies a stale-pin (423) status drops the pin id and re-pulls without it, capturing the new
     * pin id from the next delivery (issue #7).
     */
    public function testHandleRePinsOnStalePin(): void
    {
        $stalePin = "NATS/1.0 423 Nats-Pin-Id Mismatch\r\nStatus: 423\r\n\r\n";
        $hStale = strlen($stalePin);
        $body = 'order-9';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): stale pin -> drop pin id and retry without it.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hStale, $hStale, $stalePin),
            // iter 2 (sid 2): re-pinned, a message arrives carrying the new pin id.
            sprintf(
                "HMSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d %d\r\nNATS/1.0\r\nNats-Pin-Id: pin-new\r\n\r\n%s\r\n",
                strlen("NATS/1.0\r\nNats-Pin-Id: pin-new\r\n\r\n"),
                strlen("NATS/1.0\r\nNats-Pin-Id: pin-new\r\n\r\n") + strlen($body),
                $body,
            ),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setGroup('g1')
            ->setIterations(2)
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        self::assertSame(1, $total);
        self::assertSame(['order-9'], $processed);

        // The first pull carries the group; after the 423 the pin id is cleared and re-pulled.
        $pullWrites = array_values(array_filter(
            $transport->writes,
            static fn (string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));
        self::assertStringContainsString('"group":"g1"', $pullWrites[0]);
    }
}
