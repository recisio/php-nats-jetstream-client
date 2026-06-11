<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

/**
 * Typed configuration for an Object Store bucket, mirroring nats.go `ObjectStoreConfig` /
 * nats.java `ObjectStoreConfiguration`. Maps semantic fields to the backing stream config that
 * {@see ObjectStoreBucket::create()} accepts.
 */
final class ObjectStoreConfig
{
    /**
     * @param int|null    $ttlSeconds   Object time-to-live in seconds (maps to stream `max_age`).
     * @param int|null    $maxBytes     Max total bytes the bucket may hold (`max_bytes`).
     * @param string|null $storage      Storage backend: `file` or `memory` (`storage`).
     * @param int|null    $replicas     Number of replicas (`num_replicas`).
     * @param string|null $compression  Stream compression: `s2` or `none` (`compression`).
     * @param string|null $description  Human-readable description.
     * @param array{cluster?:string,tags?:list<string>}|null $placement Cluster placement (`placement`).
     */
    public function __construct(
        public readonly ?int $ttlSeconds = null,
        public readonly ?int $maxBytes = null,
        public readonly ?string $storage = null,
        public readonly ?int $replicas = null,
        public readonly ?string $compression = null,
        public readonly ?string $description = null,
        public readonly ?array $placement = null,
    ) {}

    /**
     * Resolves the backing stream-config fragment implied by these settings.
     *
     * @return array<string,mixed>
     */
    public function toStreamConfig(): array
    {
        $config = [];

        if ($this->ttlSeconds !== null) {
            $config['max_age'] = $this->ttlSeconds * 1_000_000_000;
        }
        if ($this->maxBytes !== null) {
            $config['max_bytes'] = $this->maxBytes;
        }
        if ($this->storage !== null) {
            $config['storage'] = $this->storage;
        }
        if ($this->replicas !== null) {
            $config['num_replicas'] = $this->replicas;
        }
        if ($this->compression !== null) {
            $config['compression'] = $this->compression;
        }
        if ($this->description !== null) {
            $config['description'] = $this->description;
        }
        if ($this->placement !== null) {
            $config['placement'] = $this->placement;
        }

        return $config;
    }
}
