<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;

/**
 * Helper for building scheduler expressions accepted by current NATS behavior.
 */
final class Schedule
{
    /**
     * Formats a schedule expression accepted by current NATS scheduler behavior.
     */
    public static function at(DateTimeInterface $when): string
    {
        $utc = new DateTimeZone('UTC');
        $value = DateTimeImmutable::createFromInterface($when)->setTimezone($utc);

        return '@at ' . $value->format('Y-m-d\TH:i:s\Z');
    }

    /**
     * Formats a schedule expression from a Unix timestamp.
     */
    public static function atTimestamp(int $timestamp): string
    {
        $utc = new DateTimeZone('UTC');
        $when = (new DateTimeImmutable('@' . $timestamp))->setTimezone($utc);

        return self::at($when);
    }

    /**
     * Formats a recurring "@every <interval>" schedule. Accepts an integer number of seconds or a
     * Go-style duration string (e.g. "1h", "30m", "1h30m"). The server applies the schedule starting
     * from the publish time and re-delivers on each interval.
     */
    public static function every(int|string $interval): string
    {
        if (is_int($interval)) {
            if ($interval <= 0) {
                throw new \InvalidArgumentException('Schedule interval must be a positive number of seconds');
            }

            return '@every ' . $interval . 's';
        }

        $interval = trim($interval);
        if ($interval === '') {
            throw new \InvalidArgumentException('Schedule interval must not be empty');
        }

        return '@every ' . $interval;
    }

    /**
     * Validates and returns a 6-field (seconds-resolution) cron schedule expression as accepted by
     * the NATS scheduler, e.g. "0 0 * * * *" (every hour on the hour). The fields are
     * second minute hour day-of-month month day-of-week.
     */
    public static function cron(string $expression): string
    {
        $expression = trim($expression);
        $fields = $expression === '' ? [] : preg_split('/\s+/', $expression);

        if ($fields === false || count($fields) !== 6) {
            throw new \InvalidArgumentException(
                'Cron expression must have 6 space-separated fields (second minute hour day-of-month month day-of-week)',
            );
        }

        return $expression;
    }
}
