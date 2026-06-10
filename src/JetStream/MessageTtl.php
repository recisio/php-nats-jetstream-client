<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use IDCT\NATS\Exception\JetStreamException;

/**
 * Formats the per-message TTL header value (`Nats-TTL`, ADR-43). A stream must be created with
 * `allow_msg_ttl` enabled for the server to honor it.
 */
final class MessageTtl
{
    /**
     * Formats a TTL into its `Nats-TTL` header value. Accepts an integer number of seconds (>= 1),
     * a Go-style duration string (e.g. "30s", "1h", "1h30m"), or "never" (the message never expires,
     * even from the stream age limit). Empty, negative, and zero values are rejected; any other
     * duration string is passed through for the NATS server to validate (it rejects sub-second TTLs).
     */
    public static function format(int|string $ttl): string
    {
        if (is_int($ttl)) {
            if ($ttl < 1) {
                throw new JetStreamException('Per-message TTL must be at least 1 second');
            }

            return $ttl . 's';
        }

        $ttl = trim($ttl);
        if ($ttl === '') {
            throw new JetStreamException('Per-message TTL must not be empty');
        }

        // "never" disables expiry entirely (case-insensitive).
        if (strcasecmp($ttl, 'never') === 0) {
            return 'never';
        }

        // A bare integer string is treated as seconds.
        if (preg_match('/^\d+$/', $ttl) === 1) {
            if ((int) $ttl < 1) {
                throw new JetStreamException('Per-message TTL must be at least 1 second');
            }

            return $ttl . 's';
        }

        // A negative duration is never valid.
        if (str_starts_with($ttl, '-')) {
            throw new JetStreamException('Per-message TTL must be positive');
        }

        // A zero-valued duration (e.g. "0s", "0ms", "0.0h") is rejected like the integer 0.
        if (preg_match('/^0+(?:\.0+)?(?:ns|us|µs|ms|s|m|h)?$/', $ttl) === 1) {
            throw new JetStreamException('Per-message TTL must be at least 1 second');
        }

        // Any other Go-style duration string is passed through for the server to validate.
        return $ttl;
    }
}
