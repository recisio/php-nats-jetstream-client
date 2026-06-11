<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Configuration;

use IDCT\NATS\JetStream\Enum\AckPolicy;
use IDCT\NATS\JetStream\Enum\DeliverPolicy;
use IDCT\NATS\JetStream\Enum\ReplayPolicy;

/**
 * Fluent, typed builder for a JetStream consumer configuration, mirroring nats.go `ConsumerConfig` /
 * nats.java `ConsumerConfiguration`. Produces the `config` payload accepted by
 * {@see \IDCT\NATS\JetStream\JetStreamContext::addConsumer()}.
 *
 * A durable name makes the consumer durable; omit it for an ephemeral consumer. Durations are accepted
 * in milliseconds and emitted as the nanoseconds the server expects.
 */
final class ConsumerConfiguration
{
    private ?string $durableName = null;

    /** @var array<string,mixed> */
    private array $config = [];

    public static function create(): self
    {
        return new self();
    }

    /** The durable name, or null for an ephemeral consumer. */
    public function getName(): ?string
    {
        return $this->durableName;
    }

    public function durable(string $name): self
    {
        $this->durableName = $name;
        $this->config['durable_name'] = $name;

        return $this;
    }

    public function ackPolicy(AckPolicy $policy): self
    {
        $this->config['ack_policy'] = $policy->value;

        return $this;
    }

    public function deliverPolicy(DeliverPolicy $policy): self
    {
        $this->config['deliver_policy'] = $policy->value;

        return $this;
    }

    public function replayPolicy(ReplayPolicy $policy): self
    {
        $this->config['replay_policy'] = $policy->value;

        return $this;
    }

    /** @param int $ms Ack wait in milliseconds (emitted as `ack_wait` nanoseconds). */
    public function ackWait(int $ms): self
    {
        $this->config['ack_wait'] = $ms * 1_000_000;

        return $this;
    }

    public function maxDeliver(int $maxDeliver): self
    {
        $this->config['max_deliver'] = $maxDeliver;

        return $this;
    }

    public function maxAckPending(int $maxAckPending): self
    {
        $this->config['max_ack_pending'] = $maxAckPending;

        return $this;
    }

    public function filterSubject(string $subject): self
    {
        $this->config['filter_subject'] = $subject;

        return $this;
    }

    /** @param list<string> $subjects */
    public function filterSubjects(array $subjects): self
    {
        $this->config['filter_subjects'] = $subjects;

        return $this;
    }

    public function deliverSubject(string $subject): self
    {
        $this->config['deliver_subject'] = $subject;

        return $this;
    }

    public function deliverGroup(string $group): self
    {
        $this->config['deliver_group'] = $group;

        return $this;
    }

    public function optStartSequence(int $seq): self
    {
        $this->config['opt_start_seq'] = $seq;

        return $this;
    }

    public function optStartTime(string $rfc3339): self
    {
        $this->config['opt_start_time'] = $rfc3339;

        return $this;
    }

    public function headersOnly(bool $headersOnly = true): self
    {
        $this->config['headers_only'] = $headersOnly;

        return $this;
    }

    public function memoryStorage(bool $memory = true): self
    {
        $this->config['mem_storage'] = $memory;

        return $this;
    }

    public function replicas(int $replicas): self
    {
        $this->config['num_replicas'] = $replicas;

        return $this;
    }

    /** @param int $ms Inactivity threshold in milliseconds (emitted as `inactive_threshold` nanoseconds). */
    public function inactiveThreshold(int $ms): self
    {
        $this->config['inactive_threshold'] = $ms * 1_000_000;

        return $this;
    }

    /** @param list<int> $backoffMs Per-redelivery backoff intervals in ms (emitted as nanoseconds). */
    public function backoff(array $backoffMs): self
    {
        $this->config['backoff'] = array_map(static fn(int $ms): int => $ms * 1_000_000, $backoffMs);

        return $this;
    }

    public function description(string $description): self
    {
        $this->config['description'] = $description;

        return $this;
    }

    /** @param array<string,string> $metadata */
    public function metadata(array $metadata): self
    {
        $this->config['metadata'] = $metadata;

        return $this;
    }

    /** Escape hatch for any consumer-config field not covered by a dedicated setter. */
    public function set(string $key, mixed $value): self
    {
        $this->config[$key] = $value;

        return $this;
    }

    /**
     * Builds the consumer-config payload.
     *
     * @return array<string,mixed>
     */
    public function toArray(): array
    {
        return $this->config;
    }
}
