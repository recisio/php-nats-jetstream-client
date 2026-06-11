<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

use IDCT\NATS\Core\NatsMessage;

/**
 * Parsed JetStream delivery metadata carried by a message's `$JS.ACK` reply subject.
 *
 * Mirrors nats.go `MsgMetadata` and nats.java `NatsJetStreamMetaData`: stream/consumer sequences,
 * redelivery count, pending backlog, server timestamp, and (for domain/account-prefixed deliveries)
 * the JetStream domain.
 */
final class JsMessageMetadata
{
    /**
     * @param string      $stream            Stream the message was stored in.
     * @param string      $consumer          Consumer that delivered the message.
     * @param int         $numDelivered      Delivery count (1 on first delivery, >1 after redelivery).
     * @param int         $streamSequence    Sequence of the message within the stream.
     * @param int         $consumerSequence  Sequence of this delivery within the consumer.
     * @param int         $numPending        Messages still pending for the consumer after this one.
     * @param int         $timestampNanos    Server store timestamp, in nanoseconds since the Unix epoch.
     * @param string|null $domain            JetStream domain, or null for the non-domain reply form.
     */
    public function __construct(
        public readonly string $stream,
        public readonly string $consumer,
        public readonly int $numDelivered,
        public readonly int $streamSequence,
        public readonly int $consumerSequence,
        public readonly int $numPending,
        public readonly int $timestampNanos,
        public readonly ?string $domain = null,
    ) {}

    /**
     * Parses metadata from a delivered message's `$JS.ACK` reply subject, or returns null when the
     * message was not delivered by a JetStream consumer (no parseable ack subject).
     */
    public static function fromMessage(NatsMessage $message): ?self
    {
        if ($message->replyTo === null) {
            return null;
        }

        $parts = explode('.', $message->replyTo);
        if ($parts[0] !== '$JS' || ($parts[1] ?? null) !== 'ACK') {
            return null;
        }

        // Two ack reply-subject shapes (plus a 12-token variant = the domain form + a trailing random
        // token). Token offsets, where v2/12 is the domain-qualified base:
        //   9 tokens:  $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        //  11/12 tokens: $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>[.<rand>]
        $base = match (count($parts)) {
            9 => 2,
            11, 12 => 4,
            default => null,
        };

        if ($base === null) {
            return null;
        }

        $domain = $base === 4 ? $parts[2] : null;
        if ($domain === '_') {
            // The server uses "_" as the placeholder domain when none is configured.
            $domain = null;
        }

        return new self(
            stream: $parts[$base],
            consumer: $parts[$base + 1],
            numDelivered: (int) $parts[$base + 2],
            streamSequence: (int) $parts[$base + 3],
            consumerSequence: (int) $parts[$base + 4],
            numPending: (int) $parts[$base + 6],
            timestampNanos: (int) $parts[$base + 5],
            domain: $domain,
        );
    }

    /**
     * The server store timestamp as a UTC {@see \DateTimeImmutable} (microsecond resolution; the
     * nanosecond remainder is available via {@see $timestampNanos}).
     */
    public function timestamp(): \DateTimeImmutable
    {
        $seconds = intdiv($this->timestampNanos, 1_000_000_000);
        $micros = intdiv($this->timestampNanos % 1_000_000_000, 1_000);

        $dt = \DateTimeImmutable::createFromFormat(
            'U.u',
            sprintf('%d.%06d', $seconds, $micros),
            new \DateTimeZone('UTC'),
        );

        // createFromFormat returns false only on a malformed format string, which cannot happen for
        // the fixed numeric format above; fall back defensively to keep the return type total.
        return $dt !== false ? $dt : (new \DateTimeImmutable('@' . $seconds));
    }
}
