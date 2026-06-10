<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * Immutable acknowledgment returned by JetStream publish APIs.
 */
final class PubAck
{
    /**
     * Represents an acknowledgment for a JetStream publish request.
     *
     * @param string $stream Stream that accepted the publish.
     * @param int $seq Assigned stream sequence number for the stored message.
     * @param bool $duplicate Indicates server detected duplicate publish by message ID.
     * @param array<string,mixed> $raw Full publish acknowledgment payload.
     * @param int|null $batchCount Number of messages committed (atomic batch commit ack only).
     * @param string|null $batchId Batch id echoed by an atomic batch commit ack (else null).
     */
    public function __construct(
        public readonly string $stream,
        public readonly int $seq,
        public readonly bool $duplicate,
        /** @var array<string,mixed> */
        public readonly array $raw,
        public readonly ?int $batchCount = null,
        public readonly ?string $batchId = null,
    ) {}

    /**
     * Hydrates publish acknowledgment from JetStream API JSON.
     *
     * @param array<string,mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            stream: (string) ($data['stream'] ?? ''),
            seq: (int) ($data['seq'] ?? 0),
            duplicate: (bool) ($data['duplicate'] ?? false),
            raw: $data,
            batchCount: isset($data['count']) ? (int) $data['count'] : null,
            batchId: isset($data['batch']) ? (string) $data['batch'] : null,
        );
    }
}
