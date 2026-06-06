<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

use Amp\Future;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Models\StreamInfo;

use function Amp\async;

/**
 * Implements NATS JetStream Object Store bucket operations using the official on-wire layout
 * (meta subjects keyed by base64url(name), chunks under a per-object NUID subject, SHA-256 digest
 * in base64url, and rollup meta), so buckets interoperate with the `nats` CLI and other clients.
 */
final class ObjectStoreBucket
{
    private const DEFAULT_CHUNK_SIZE = 131072; // 128 KiB

    /**
     * Number of chunks pulled per download batch. Bounds peak memory to roughly
     * DOWNLOAD_BATCH_CHUNKS * chunkSize while replacing the previous one-request-per-chunk pattern,
     * so large objects download in O(chunks / DOWNLOAD_BATCH_CHUNKS) round-trips instead of O(chunks).
     */
    private const DOWNLOAD_BATCH_CHUNKS = 64;

    /** Per-batch pull expiry in milliseconds for chunk downloads. */
    private const DOWNLOAD_BATCH_EXPIRES_MS = 10000;

    /**
     * Creates an Object Store bucket context bound to a client and bucket name.
     *
     * @param NatsClient $client Connected client used for chunk publish and metadata retrieval operations.
     * @param JetStreamContext $jetStream JetStream context used to manage backing object-store streams.
     * @param string $bucket Object Store bucket name used to build stream and metadata/chunk subject prefixes.
     * @param int $chunkSize Chunk size in bytes used when splitting object payloads before publishing chunk messages.
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly JetStreamContext $jetStream,
        private readonly string $bucket,
        private readonly int $chunkSize = self::DEFAULT_CHUNK_SIZE,
    ) {}

    /**
     * Creates or updates the underlying Object Store stream.
     *
     * @param array<string,mixed> $options
     * @return Future<StreamInfo>
     */
    public function create(array $options = []): Future
    {
        return async(function () use ($options): StreamInfo {
            $defaults = [
                'description' => 'Object Store bucket ' . $this->bucket,
                'allow_direct' => true,
                'allow_rollup_hdrs' => true,
                'discard' => 'new',
            ];

            return $this->jetStream->createStream(
                $this->streamName(),
                [$this->chunkPrefix() . '>', $this->metaPrefix() . '>'],
                array_merge($defaults, $options),
            )->await();
        });
    }

    /**
     * Deletes the underlying Object Store stream.
     *
     * @return Future<bool>
     */
    public function deleteBucket(): Future
    {
        return $this->jetStream->deleteStream($this->streamName());
    }

    /**
     * Stores an object payload and publishes metadata, purging any previous revision's chunks.
     *
     * @param array<string,string> $metadata
     * @return Future<ObjectInfo>
     */
    public function put(string $name, string $data, array $metadata = []): Future
    {
        return async(function () use ($name, $data, $metadata): ObjectInfo {
            $this->assertValidName($name);

            $previous = $this->lookupExisting($name);

            $nuid = $this->nuid();
            $chunkSubject = $this->chunkSubjectForNuid($nuid);
            $totalSize = strlen($data);
            $chunks = 0;

            if ($totalSize <= $this->chunkSize) {
                $this->jetStream->publish($chunkSubject, $data)->await();
                $chunks = 1;
            } else {
                $offset = 0;
                while ($offset < $totalSize) {
                    $chunk = substr($data, $offset, $this->chunkSize);
                    $this->jetStream->publish($chunkSubject, $chunk)->await();
                    $offset += strlen($chunk);
                    $chunks++;
                }
            }

            $info = [
                'name' => $name,
                'bucket' => $this->bucket,
                'nuid' => $nuid,
                'size' => $totalSize,
                'chunks' => $chunks,
                'digest' => $this->digestOf($data),
                'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
                'deleted' => false,
                'options' => ['max_chunk_size' => $this->chunkSize],
                'metadata' => $metadata,
            ];

            $this->publishMeta($name, $info);

            // Best-effort cleanup of the previous revision's chunks (rollup keeps only the latest
            // meta, but chunk subjects are NUID-specific and must be purged explicitly).
            if ($previous !== null && $previous->nuid !== '' && $previous->nuid !== $nuid) {
                $this->purgeChunks($previous->nuid);
            }

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Retrieves object metadata and payload.
     *
     * @return Future<ObjectData|null>
     */
    public function get(string $name): Future
    {
        return async(function () use ($name): ?ObjectData {
            $info = $this->info($name)->await();
            if ($info === null) {
                return null;
            }

            if ($info->deleted) {
                return new ObjectData($info, null);
            }

            $assembled = '';
            $actualDigest = $this->streamChunks($info, static function (string $chunk) use (&$assembled): void {
                $assembled .= $chunk;
            });

            $this->verifyDigest($info, $actualDigest);

            return new ObjectData($info, $assembled);
        });
    }

    /**
     * Streams object payload to a callback chunk-by-chunk without buffering the whole object.
     *
     * The callback is invoked once per stored chunk as it is downloaded, so large objects do not
     * have to be held in memory. The content digest is computed incrementally and verified after
     * the final chunk.
     *
     * @param callable(string):void $chunkHandler
     * @return Future<ObjectInfo|null>
     */
    public function getToCallback(string $name, callable $chunkHandler): Future
    {
        return async(function () use ($name, $chunkHandler): ?ObjectInfo {
            $info = $this->info($name)->await();
            if ($info === null) {
                return null;
            }

            if ($info->deleted) {
                return $info;
            }

            $actualDigest = $this->streamChunks($info, $chunkHandler);
            $this->verifyDigest($info, $actualDigest);

            return $info;
        });
    }

    /**
     * Downloads an object's chunks via a transient ephemeral consumer, forwarding each chunk to the
     * callback and computing the SHA-256 content digest incrementally. Returns the computed digest
     * string ("SHA-256=...") so callers can verify integrity without re-buffering the payload.
     *
     * Chunks are pulled in bounded batches (DOWNLOAD_BATCH_CHUNKS at a time) rather than one pull
     * request per chunk, so large objects download in far fewer round-trips while peak memory stays
     * bounded (the whole object is never held in memory). JetStream delivers chunks of a single
     * filtered consumer in stream order, preserving in-order assembly.
     *
     * @param callable(string):void $onChunk
     */
    private function streamChunks(ObjectInfo $info, callable $onChunk): string
    {
        $expected = $info->chunks;
        $remaining = max(1, $expected);
        $received = 0;
        $hashContext = hash_init('sha256');

        $consumerName = null;
        try {
            // A read-only download uses ack_policy=none: there is nothing to ack, and an explicit
            // policy risks ack_wait redelivery on a slow link re-hashing a chunk into a digest mismatch.
            $consumer = $this->jetStream->createEphemeralConsumer(
                $this->streamName(),
                $this->chunkSubjectForNuid($info->nuid),
                ['deliver_policy' => 'all', 'ack_policy' => 'none'],
            )->await();
            $consumerName = $consumer->name;

            while ($remaining > 0) {
                $batch = min(self::DOWNLOAD_BATCH_CHUNKS, $remaining);

                try {
                    $messages = $this->jetStream->fetchBatch(
                        $this->streamName(),
                        $consumerName,
                        $batch,
                        self::DOWNLOAD_BATCH_EXPIRES_MS,
                    )->await();
                } catch (JetStreamException $e) {
                    if ($e->getCode() === 408) {
                        // No further chunks available; the completeness check below catches a shortfall.
                        break;
                    }

                    throw $e;
                }

                foreach ($messages as $message) {
                    hash_update($hashContext, $message->payload);
                    $onChunk($message->payload);
                    ++$received;
                }

                // Decrement by what actually arrived and pull again for the rest. A short batch is
                // not treated as "drained" here: fetchBatch() can return fewer than requested when
                // its window expires mid-delivery on a slow link, so the next iteration keeps going.
                // The empty case surfaces as a 408 above and breaks the loop.
                $remaining -= count($messages);
            }
        } finally {
            if ($consumerName !== null && $consumerName !== '') {
                try {
                    $this->jetStream->deleteConsumer($this->streamName(), $consumerName)->await();
                } catch (JetStreamException) {
                    // Best-effort ephemeral consumer cleanup.
                }
            }
        }

        // Digest-independent completeness gate: a truncated download must fail even when the
        // metadata carries no digest to verify against (e.g. an object written by another client).
        if ($expected > 0 && $received < $expected) {
            throw new JetStreamException(sprintf(
                'Incomplete object download: expected %d chunks, received %d',
                $expected,
                $received,
            ));
        }

        return 'SHA-256=' . $this->base64Url(hash_final($hashContext, true));
    }

    /**
     * Verifies a downloaded object's computed digest against the digest recorded in its metadata.
     */
    private function verifyDigest(ObjectInfo $info, string $actualDigest): void
    {
        if ($info->digest !== '' && $info->digest !== $actualDigest) {
            throw new JetStreamException(
                'Object digest mismatch: expected ' . $info->digest . ', got ' . $actualDigest,
            );
        }
    }

    /**
     * Retrieves object metadata only.
     *
     * @return Future<ObjectInfo|null>
     */
    public function info(string $name): Future
    {
        return async(function () use ($name): ?ObjectInfo {
            $this->assertValidName($name);

            try {
                $response = $this->requestStreamMessage($this->metaSubject($name));
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    return null;
                }

                throw $e;
            }

            /** @var array<string,mixed>|null $message */
            $message = is_array($response['message'] ?? null) ? $response['message'] : null;
            if ($message === null) {
                return null;
            }

            $metadata = $this->decodeMetadataFromApiMessage($message);
            if ($metadata === null) {
                return null;
            }

            return ObjectInfo::fromArray($this->bucket, $metadata);
        });
    }

    /**
     * Marks an object as deleted by writing a metadata tombstone and purging its chunks.
     *
     * @return Future<ObjectInfo>
     */
    public function delete(string $name): Future
    {
        return async(function () use ($name): ObjectInfo {
            $this->assertValidName($name);

            $previous = $this->lookupExisting($name);

            $info = [
                'name' => $name,
                'bucket' => $this->bucket,
                'nuid' => '',
                'size' => 0,
                'chunks' => 0,
                'digest' => '',
                'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
                'deleted' => true,
                'options' => ['max_chunk_size' => $this->chunkSize],
                'metadata' => [],
            ];

            $this->publishMeta($name, $info);

            if ($previous !== null && $previous->nuid !== '') {
                $this->purgeChunks($previous->nuid);
            }

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Watches metadata subjects and emits object metadata updates.
     *
     * @param callable(ObjectInfo):void $handler
     * @return Future<int>
     */
    public function watch(callable $handler, string $pattern = '>'): Future
    {
        return async(function () use ($handler, $pattern): int {
            return $this->client->subscribe($this->metaPrefix() . $pattern, function (NatsMessage $message) use ($handler): void {
                $metadata = json_decode($message->payload, true);
                if (!is_array($metadata)) {
                    // Tolerate a malformed meta delivery instead of throwing out of the dispatch loop,
                    // which would abort delivery of buffered frames for other subscriptions too.
                    return;
                }

                /** @var array<string,mixed> $metadata */
                $handler(ObjectInfo::fromArray($this->bucket, $metadata));
            })->await();
        });
    }

    /**
     * Lists latest metadata records for objects in this bucket.
     *
     * Enumerates only the meta subjects (via a subjects filter) and reads the latest record per
     * subject, so cost is O(objects) rather than O(all stream messages).
     *
     * @return Future<list<ObjectInfo>>
     */
    public function list(bool $includeDeleted = false): Future
    {
        return async(function () use ($includeDeleted): array {
            $subjects = $this->metaSubjects();
            if ($subjects === []) {
                return [];
            }

            // Read the latest meta record per object CONCURRENTLY via the Direct Get API (served by
            // any replica; the round-trips overlap) instead of N+1 serial leader reads.
            $lookups = [];
            foreach ($subjects as $subject) {
                $lookups[] = async(function () use ($subject): ?ObjectInfo {
                    try {
                        $message = $this->jetStream->directGetLastMessageForSubject($this->streamName(), $subject)->await();
                    } catch (JetStreamException $e) {
                        if ($e->getCode() === 404) {
                            return null;
                        }

                        throw $e;
                    }

                    // The Direct Get body is the raw meta JSON (not the base64 STREAM.MSG.GET envelope).
                    /** @var array<string,mixed>|null $metadata */
                    $metadata = json_decode($message->payload, true);
                    if (!is_array($metadata)) {
                        return null;
                    }

                    $info = ObjectInfo::fromArray($this->bucket, $metadata);

                    return $info->name === '' ? null : $info;
                });
            }

            /** @var list<?ObjectInfo> $infos */
            $infos = Future\await($lookups);

            $result = [];
            foreach ($infos as $info) {
                if ($info === null) {
                    continue;
                }

                if (!$includeDeleted && $info->deleted) {
                    continue;
                }

                $result[] = $info;
            }

            return $result;
        });
    }

    /**
     * Returns object bucket status derived from stream state.
     *
     * @return Future<array<string,mixed>>
     */
    public function getStatus(): Future
    {
        return async(function (): array {
            $stream = $this->jetStream->getStream($this->streamName())->await();
            /** @var array<string,mixed> $state */
            $state = is_array($stream->raw['state'] ?? null) ? $stream->raw['state'] : [];

            return [
                'bucket' => $this->bucket,
                'stream' => $this->streamName(),
                'messages' => (int) ($state['messages'] ?? 0),
                'last_sequence' => (int) ($state['last_seq'] ?? 0),
                'bytes' => (int) ($state['bytes'] ?? 0),
                'subjects' => is_array($state['subjects'] ?? null) ? $state['subjects'] : [],
            ];
        });
    }

    /**
     * Returns Object Store stream name.
     */
    public function streamName(): string
    {
        return 'OBJ_' . $this->bucket;
    }

    /**
     * Returns chunk subject prefix.
     */
    public function chunkPrefix(): string
    {
        return '$O.' . $this->bucket . '.C.';
    }

    /**
     * Returns metadata subject prefix.
     */
    public function metaPrefix(): string
    {
        return '$O.' . $this->bucket . '.M.';
    }

    /**
     * Resolves the metadata subject for an object name (official base64url-of-name encoding).
     */
    private function metaSubject(string $name): string
    {
        return $this->metaPrefix() . $this->encodeName($name);
    }

    /**
     * Resolves the chunk subject for a given object NUID.
     */
    private function chunkSubjectForNuid(string $nuid): string
    {
        return $this->chunkPrefix() . $nuid;
    }

    /**
     * Looks up an existing object's metadata, returning null when absent (best-effort, swallows
     * not-found and lookup errors so a fresh put/delete is never blocked by it).
     */
    private function lookupExisting(string $name): ?ObjectInfo
    {
        try {
            return $this->info($name)->await();
        } catch (JetStreamException) {
            return null;
        }
    }

    /**
     * Publishes an object metadata record with a rollup header so only the latest record per object
     * is retained, then validates the publish acknowledgement.
     *
     * @param array<string,mixed> $info
     */
    private function publishMeta(string $name, array $info): void
    {
        $message = $this->client->requestWithHeaders(
            $this->metaSubject($name),
            json_encode($info, JSON_THROW_ON_ERROR),
            ['Nats-Rollup' => 'sub'],
        )->await();

        /** @var array<string,mixed> $data */
        $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            throw new JetStreamException(
                (string) ($error['description'] ?? 'JetStream publish error'),
                (int) ($error['code'] ?? 0),
            );
        }
    }

    /**
     * Best-effort purge of all chunk messages for a given object NUID.
     */
    private function purgeChunks(string $nuid): void
    {
        try {
            $this->jetStream->purgeStream(
                $this->streamName(),
                ['filter' => $this->chunkSubjectForNuid($nuid)],
            )->await();
        } catch (JetStreamException) {
            // Best-effort cleanup; never fail the surrounding operation on purge errors.
        }
    }

    /**
     * Enumerates the meta subjects currently present in the bucket using a subjects filter.
     *
     * @return list<string>
     */
    private function metaSubjects(): array
    {
        $apiSubject = JetStreamApi::STREAM_INFO_PREFIX . $this->streamName();
        $payload = json_encode(['subjects_filter' => $this->metaPrefix() . '>'], JSON_THROW_ON_ERROR);
        $message = $this->client->request($apiSubject, $payload)->await();

        /** @var array<string,mixed> $data */
        $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            throw new JetStreamException(
                (string) ($error['description'] ?? 'JetStream API error'),
                (int) ($error['code'] ?? 0),
            );
        }

        /** @var array<string,mixed> $state */
        $state = is_array($data['state'] ?? null) ? $data['state'] : [];
        /** @var array<string,int> $subjects */
        $subjects = is_array($state['subjects'] ?? null) ? $state['subjects'] : [];

        return array_map('strval', array_keys($subjects));
    }

    /**
     * @return array<string,mixed>
     */
    private function requestStreamMessage(string $subject): array
    {
        $apiSubject = JetStreamApi::STREAM_MSG_GET_PREFIX . $this->streamName();
        $payload = json_encode(['last_by_subj' => $subject], JSON_THROW_ON_ERROR);
        $message = $this->client->request($apiSubject, $payload)->await();

        /** @var array<string,mixed> $data */
        $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $description = (string) ($error['description'] ?? 'JetStream API error');
            $code = (int) ($error['code'] ?? 0);
            throw new JetStreamException($description, $code);
        }

        return $data;
    }

    /**
     * @param array<string,mixed> $message
     * @return array<string,mixed>|null
     */
    private function decodeMetadataFromApiMessage(array $message): ?array
    {
        $encodedData = (string) ($message['data'] ?? '');
        if ($encodedData === '') {
            return null;
        }

        $decoded = base64_decode($encodedData, true);
        if ($decoded === false || $decoded === '') {
            return null;
        }

        /** @var array<string,mixed>|null $metadata */
        $metadata = json_decode($decoded, true);

        return is_array($metadata) ? $metadata : null;
    }

    /**
     * Computes the official Object Store content digest ("SHA-256=" + base64url).
     */
    private function digestOf(string $data): string
    {
        return 'SHA-256=' . $this->base64Url(hash('sha256', $data, true));
    }

    /**
     * Encodes an object name into its meta-subject token using URL-safe base64 (official layout).
     */
    private function encodeName(string $name): string
    {
        return $this->base64Url($name);
    }

    /**
     * URL-safe base64 encoding (with padding), matching the official Object Store encoding.
     */
    private function base64Url(string $bytes): string
    {
        return strtr(base64_encode($bytes), '+/', '-_');
    }

    /**
     * Generates a unique object NUID used as the chunk subject token and stored in metadata.
     */
    private function nuid(): string
    {
        return bin2hex(random_bytes(11));
    }

    /**
     * Validates object names. Names are base64url-encoded into the meta subject, so any non-empty
     * name is acceptable (the encoding keeps the wire subject valid).
     */
    private function assertValidName(string $name): void
    {
        if ($name === '') {
            throw new JetStreamException('Invalid object name');
        }
    }
}
