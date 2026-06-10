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
     * even from the stream age limit). Sub-second, zero, negative, or empty values are rejected.
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

        // A bare integer string is treated as seconds; everything else (a Go duration or "never")
        // is passed through for the server to validate.
        if (preg_match('/^\d+$/', $ttl) === 1) {
            if ((int) $ttl < 1) {
                throw new JetStreamException('Per-message TTL must be at least 1 second');
            }

            return $ttl . 's';
        }

        return $ttl;
    }
}
