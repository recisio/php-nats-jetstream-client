<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use PHPUnit\Framework\TestCase;

final class NatsOptionsTest extends TestCase
{
    public function testFirstServerReturnsConfiguredFirstEndpoint(): void
    {
        $options = new NatsOptions(servers: ['nats://a:4222', 'nats://b:4222']);

        self::assertSame('nats://a:4222', $options->firstServer());
    }

    public function testFirstServerFallsBackWhenServersListIsEmpty(): void
    {
        $options = new NatsOptions(servers: []);

        self::assertSame('nats://127.0.0.1:4222', $options->firstServer());
    }

    public function testRejectsNonPositiveConnectTimeout(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('connectTimeoutMs');
        new NatsOptions(connectTimeoutMs: 0);
    }

    public function testRejectsNonPositiveRequestTimeout(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('requestTimeoutMs');
        new NatsOptions(requestTimeoutMs: 0);
    }

    public function testRejectsZeroMaxPendingMessages(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxPendingMessagesPerSubscription');
        new NatsOptions(maxPendingMessagesPerSubscription: 0);
    }

    public function testRejectsNegativeReconnectValue(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('reconnectDelayMs');
        new NatsOptions(reconnectDelayMs: -1);
    }

    public function testAllowsDisabledHeartbeatAndEmptyServers(): void
    {
        // pingIntervalSeconds <= 0 disables the heartbeat, maxPingsOut 0 is aggressive-but-valid, and
        // an empty servers list falls back to the default — all legitimate, so none must be rejected.
        $options = new NatsOptions(servers: [], pingIntervalSeconds: 0, maxPingsOut: 0);

        self::assertSame(0, $options->pingIntervalSeconds);
        self::assertSame(0, $options->maxPingsOut);
    }
}
