<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\JetStream\ObjectStore\ObjectStoreWatchOptions;
use PHPUnit\Framework\TestCase;

/**
 * Covers the deliver-policy mapping for ObjectStore watch options (#98), mirroring the reference
 * ObjectStore.Watch matrix.
 */
final class ObjectStoreWatchOptionsTest extends TestCase
{
    public function testDefaultOptionsReplayCurrentStateThenFollow(): void
    {
        // No flags set => reference "snapshot then follow" (current metadata of every object, then live).
        $config = (new ObjectStoreWatchOptions())->toConsumerConfig();

        self::assertSame('last_per_subject', $config['deliver_policy']);
        self::assertSame('none', $config['ack_policy']);
    }

    public function testUpdatesOnlyRequestsNewDeliverPolicy(): void
    {
        $config = (new ObjectStoreWatchOptions(updatesOnly: true))->toConsumerConfig();

        self::assertSame('new', $config['deliver_policy']);
    }

    public function testIncludeHistoryRequestsAllDeliverPolicy(): void
    {
        $config = (new ObjectStoreWatchOptions(includeHistory: true))->toConsumerConfig();

        self::assertSame('all', $config['deliver_policy']);
    }

    public function testIncludeHistoryTakesPrecedenceOverUpdatesOnly(): void
    {
        $config = (new ObjectStoreWatchOptions(updatesOnly: true, includeHistory: true))->toConsumerConfig();

        self::assertSame('all', $config['deliver_policy']);
    }
}
