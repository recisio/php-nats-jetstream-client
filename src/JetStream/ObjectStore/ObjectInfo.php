<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

/**
 * Immutable metadata for an Object Store object revision.
 */
final class ObjectInfo
{
    /**
     * Represents Object Store metadata for a single object revision.
     *
     * @param string $bucket Object Store bucket name.
     * @param string $name Object name/key in the bucket.
     * @param int $size Total object size in bytes.
     * @param int $chunks Number of chunk messages used to store object bytes.
     * @param string $digest Server-provided content digest for integrity checks.
     * @param string $modified RFC3339 timestamp of last object modification.
     * @param bool $deleted Whether this metadata represents a deleted tombstone.
     * @param string $nuid Object NUID; chunks are stored under `$O.<bucket>.C.<nuid>` (official layout).
     * @param array<string,string> $metadata
     * @param ?int $revision Stream sequence of this metadata record when known (e.g. from watch()); null otherwise.
     * @param array{bucket?:string,name?:string}|null $link Link target when this object is a link to another
     *        object (`bucket` + `name`) or to a whole bucket (`bucket` only); null for a normal object.
     */
    public function __construct(
        public readonly string $bucket,
        public readonly string $name,
        public readonly int $size,
        public readonly int $chunks,
        public readonly string $digest,
        public readonly string $modified,
        public readonly bool $deleted,
        public readonly string $nuid,
        public readonly array $metadata = [],
        public readonly ?int $revision = null,
        public readonly ?array $link = null,
    ) {}

    /**
     * Whether this object is a link (to another object or a whole bucket) rather than stored content.
     */
    public function isLink(): bool
    {
        return $this->link !== null;
    }

    /**
     * @param array<string,mixed> $data
     */
    public static function fromArray(string $bucket, array $data, ?int $revision = null): self
    {
        /** @var array<string,string> $metadata */
        $metadata = is_array($data['metadata'] ?? null) ? array_map('strval', $data['metadata']) : [];

        // Object links travel as options.link = {bucket, name?} in the meta record.
        $link = null;
        $options = is_array($data['options'] ?? null) ? $data['options'] : [];
        if (is_array($options['link'] ?? null) && isset($options['link']['bucket'])) {
            $link = ['bucket' => (string) $options['link']['bucket']];
            if (isset($options['link']['name']) && $options['link']['name'] !== '') {
                $link['name'] = (string) $options['link']['name'];
            }
        }

        return new self(
            bucket: (string) ($data['bucket'] ?? $bucket),
            name: (string) ($data['name'] ?? ''),
            size: (int) ($data['size'] ?? 0),
            chunks: (int) ($data['chunks'] ?? 0),
            digest: (string) ($data['digest'] ?? ''),
            modified: (string) ($data['mtime'] ?? ''),
            deleted: (bool) ($data['deleted'] ?? false),
            nuid: (string) ($data['nuid'] ?? ''),
            metadata: $metadata,
            revision: $revision,
            link: $link,
        );
    }
}
