<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Configuration;

use IDCT\NATS\JetStream\Enum\DiscardPolicy;
use IDCT\NATS\JetStream\Enum\RetentionPolicy;
use IDCT\NATS\JetStream\Enum\StorageBackend;

/**
 * Fluent, typed builder for a JetStream stream configuration, mirroring nats.go `StreamConfig` /
 * nats.java `StreamConfiguration`. Produces the payload accepted by
 * {@see \IDCT\NATS\JetStream\JetStreamContext::addStream()}.
 *
 * Durations are accepted in seconds and emitted as the nanoseconds the server expects. Any field not
 * covered by a dedicated setter can be set with {@see set()}.
 */
final class StreamConfiguration
{
    /** @var list<string> */
    private array $subjects = [];

    /** @var array<string,mixed> */
    private array $config = [];

    public function __construct(private readonly string $name) {}

    public static function create(string $name): self
    {
        return new self($name);
    }

    public function name(): string
    {
        return $this->name;
    }

    public function subjects(string ...$subjects): self
    {
        $this->subjects = array_values($subjects);

        return $this;
    }

    public function retention(RetentionPolicy $policy): self
    {
        $this->config['retention'] = $policy->value;

        return $this;
    }

    public function storage(StorageBackend $storage): self
    {
        $this->config['storage'] = $storage->value;

        return $this;
    }

    public function discard(DiscardPolicy $discard): self
    {
        $this->config['discard'] = $discard->value;

        return $this;
    }

    public function maxMessages(int $maxMsgs): self
    {
        $this->config['max_msgs'] = $maxMsgs;

        return $this;
    }

    public function maxBytes(int $maxBytes): self
    {
        $this->config['max_bytes'] = $maxBytes;

        return $this;
    }

    /** @param int $seconds Max age in seconds (emitted as `max_age` nanoseconds). */
    public function maxAge(int $seconds): self
    {
        $this->config['max_age'] = $seconds * 1_000_000_000;

        return $this;
    }

    public function maxMsgSize(int $bytes): self
    {
        $this->config['max_msg_size'] = $bytes;

        return $this;
    }

    public function maxMsgsPerSubject(int $maxMsgsPerSubject): self
    {
        $this->config['max_msgs_per_subject'] = $maxMsgsPerSubject;

        return $this;
    }

    public function maxConsumers(int $maxConsumers): self
    {
        $this->config['max_consumers'] = $maxConsumers;

        return $this;
    }

    public function replicas(int $replicas): self
    {
        $this->config['num_replicas'] = $replicas;

        return $this;
    }

    /** @param int $seconds De-duplication window in seconds (emitted as `duplicate_window` nanoseconds). */
    public function duplicateWindow(int $seconds): self
    {
        $this->config['duplicate_window'] = $seconds * 1_000_000_000;

        return $this;
    }

    public function allowDirect(bool $allow = true): self
    {
        $this->config['allow_direct'] = $allow;

        return $this;
    }

    public function mirrorDirect(bool $allow = true): self
    {
        $this->config['mirror_direct'] = $allow;

        return $this;
    }

    public function allowRollupHeaders(bool $allow = true): self
    {
        $this->config['allow_rollup_hdrs'] = $allow;

        return $this;
    }

    public function denyDelete(bool $deny = true): self
    {
        $this->config['deny_delete'] = $deny;

        return $this;
    }

    public function denyPurge(bool $deny = true): self
    {
        $this->config['deny_purge'] = $deny;

        return $this;
    }

    public function sealed(bool $sealed = true): self
    {
        $this->config['sealed'] = $sealed;

        return $this;
    }

    public function compression(string $compression): self
    {
        $this->config['compression'] = $compression;

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

    /**
     * Escape hatch for any stream-config field not covered by a dedicated setter (e.g. `mirror`,
     * `sources`, `placement`, `republish`, `subject_transform`, `first_seq`).
     */
    public function set(string $key, mixed $value): self
    {
        $this->config[$key] = $value;

        return $this;
    }

    /**
     * Builds the full stream-config payload (name + subjects + configured fields).
     *
     * @return array<string,mixed>
     */
    public function toArray(): array
    {
        return $this->config + ['name' => $this->name, 'subjects' => $this->subjects];
    }
}
