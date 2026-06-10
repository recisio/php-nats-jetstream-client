<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use IDCT\NATS\JetStream\Schedule;
use PHPUnit\Framework\TestCase;

final class ScheduleTest extends TestCase
{
    /**
     * Verifies helper formats @at schedules in UTC.
     */
    public function testAtFormatsUtcExpression(): void
    {
        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $schedule = Schedule::at($when);

        self::assertSame('@at 2030-01-01T00:00:00Z', $schedule);
    }

    /**
     * Verifies helper normalizes non-UTC input to UTC schedule format.
     */
    public function testAtNormalizesTimezoneToUtc(): void
    {
        $when = new DateTimeImmutable('2030-01-01 01:00:00', new DateTimeZone('Europe/Warsaw'));

        $schedule = Schedule::at($when);

        self::assertSame('@at 2030-01-01T00:00:00Z', $schedule);
    }

    /**
     * Verifies Unix timestamp helper returns valid @at expression.
     */
    public function testAtTimestamp(): void
    {
        $schedule = Schedule::atTimestamp(1_893_456_000);

        self::assertSame('@at 2030-01-01T00:00:00Z', $schedule);
    }

    /**
     * Verifies @every from an integer number of seconds.
     */
    public function testEveryFromSeconds(): void
    {
        self::assertSame('@every 30s', Schedule::every(30));
    }

    /**
     * Verifies @every from a Go-style duration string.
     */
    public function testEveryFromDurationString(): void
    {
        self::assertSame('@every 1h30m', Schedule::every('1h30m'));
    }

    /**
     * Verifies @every rejects a non-positive interval.
     */
    public function testEveryRejectsNonPositiveSeconds(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Schedule::every(0);
    }

    /**
     * Verifies @every rejects an empty interval string.
     */
    public function testEveryRejectsEmptyString(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Schedule::every('   ');
    }

    /**
     * Verifies cron returns a valid 6-field expression unchanged.
     */
    public function testCronReturnsSixFieldExpression(): void
    {
        self::assertSame('0 0 0 * * *', Schedule::cron('0 0 0 * * *'));
    }

    /**
     * Verifies cron rejects an expression that is not 6 fields (e.g. a 5-field unix cron).
     */
    public function testCronRejectsNonSixFieldExpression(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Schedule::cron('0 0 * * *');
    }

    /**
     * Verifies predefined() normalizes an alias (with or without a leading "@") to "@alias".
     */
    public function testPredefinedNormalizesAlias(): void
    {
        self::assertSame('@daily', Schedule::predefined('daily'));
        self::assertSame('@hourly', Schedule::predefined('@hourly'));
        self::assertSame('@monthly', Schedule::predefined('MONTHLY'));
    }

    /**
     * Verifies predefined() rejects an unknown alias.
     */
    public function testPredefinedRejectsUnknownAlias(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        Schedule::predefined('fortnightly');
    }
}
