<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class BatchPublisherTest extends TestCase
{
    /**
     * Verifies a committed batch: the START (seq 1) is a request, the intermediate is fire-and-forget,
     * the final carries the commit marker as a request, all share one Nats-Batch-Id, and the commit
     * ack is parsed (issue #8, ADR-50).
     */
    public function testCommitSendsBatchHeadersAndParsesAck(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":3,"batch":"batch-xyz","count":3}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Zero-byte ack to the batch-start request (sid 1).
            "MSG _INBOX.a 1 0\r\n\r\n",
            // Commit PubAck to the commit request (sid 2).
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('batch-xyz');
        $ack = $batch
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c')
            ->commit()
            ->await();

        self::assertSame(3, $ack->batchCount);
        self::assertSame('batch-xyz', $ack->batchId);

        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));

        self::assertCount(3, $batchWrites);

        foreach ($batchWrites as $write) {
            self::assertStringContainsString('Nats-Batch-Id:batch-xyz', $write);
        }

        // START (seq 1): a request (carries an inbox), no commit marker.
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $batchWrites[0]);
        self::assertStringContainsString('Nats-Batch-Sequence:1', $batchWrites[0]);
        self::assertStringNotContainsString('Nats-Batch-Commit', $batchWrites[0]);

        // Intermediate (seq 2): fire-and-forget (no inbox after the subject, just the byte counts).
        self::assertMatchesRegularExpression('/^HPUB orders\.created \d/', $batchWrites[1]);
        self::assertStringContainsString('Nats-Batch-Sequence:2', $batchWrites[1]);
        self::assertStringNotContainsString('Nats-Batch-Commit', $batchWrites[1]);

        // Commit (seq 3): a request carrying the commit marker.
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $batchWrites[2]);
        self::assertStringContainsString('Nats-Batch-Sequence:3', $batchWrites[2]);
        self::assertStringContainsString('Nats-Batch-Commit:1', $batchWrites[2]);
    }

    /**
     * Verifies a batch-start rejection (error reply to the start request) aborts before the
     * intermediates/commit are sent (issue #8, ADR-50).
     */
    public function testCommitRejectedAtStart(): void
    {
        $startError = '{"error":{"code":400,"description":"atomic publish not enabled"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($startError), $startError),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('batch-rej')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c');

        try {
            $batch->commit()->await();
            self::fail('Expected the batch-start rejection to throw');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('atomic publish not enabled', $e->getMessage());
        }

        // Only the start message was written; no commit and no further sequence was sent.
        $allWrites = implode('', $transport->writes);
        self::assertStringNotContainsString('Nats-Batch-Commit', $allWrites);
        self::assertStringNotContainsString('Nats-Batch-Sequence:3', $allWrites);
    }

    /**
     * Verifies committing an empty batch is rejected.
     */
    public function testCommitEmptyBatchThrows(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Cannot commit an empty batch');

        $client->jetStream()->batch()->commit()->await();
    }

    /**
     * Verifies a too-long batch id is rejected.
     */
    public function testBatchRejectsOversizedId(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Batch id must be between 1 and 64 characters');

        $client->jetStream()->batch(str_repeat('x', 65));
    }

    /**
     * Verifies adding after commit is rejected.
     */
    public function testAddAfterCommitThrows(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":1,"batch":"b","count":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b');
        $batch->add('orders.created', 'a')->commit()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Cannot add to an already-committed batch');

        $batch->add('orders.created', 'b');
    }

    /**
     * Verifies an aborted batch (commit ack carries an error) surfaces as a JetStreamException.
     */
    public function testCommitAbortSurfacesError(): void
    {
        $errorAck = '{"error":{"code":400,"description":"batch consistency check failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorAck), $errorAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('batch consistency check failed');

        $client->jetStream()->batch('b')->add('orders.created', 'a')->commit()->await();
    }
}
