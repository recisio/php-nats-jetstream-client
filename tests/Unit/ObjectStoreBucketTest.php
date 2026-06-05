<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectData;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ObjectStoreBucketTest extends TestCase
{
    /** URL-safe base64 (with padding), matching the official Object Store meta-subject encoding. */
    private function encodeName(string $name): string
    {
        return strtr(base64_encode($name), '+/', '-_');
    }

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    /** @param array<string,mixed> $extra */
    private function metaGetResponse(string $name, array $extra): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);

        return (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $this->encodeName($name),
                'seq' => 2,
                'data' => base64_encode((string) json_encode($meta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);
    }

    private function notFound(): string
    {
        return '{"error":{"code":404,"description":"message not found"}}';
    }

    private function pubAck(int $seq): string
    {
        return sprintf('{"stream":"OBJ_assets","seq":%d,"duplicate":false}', $seq);
    }

    /**
     * Verifies Object Store bucket create maps to a stream with chunk+meta subjects and rollup.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"OBJ_assets","subjects":["$O.assets.C.>","$O.assets.M.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $created = $bucket->create()->await();
        $deleted = $bucket->deleteBucket()->await();

        self::assertSame('OBJ_assets', $created->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.OBJ_assets', $transport->writes[3]);
        self::assertStringContainsString('"allow_rollup_hdrs":true', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.OBJ_assets', $transport->writes[6]);
    }

    /**
     * Verifies put writes chunks under a NUID subject and rollup meta under the encoded name subject.
     */
    public function testPutUsesEncodedMetaSubjectAndNuidChunks(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // 1) existing-object lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // 2) chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            // 3) meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $stored = $client->jetStream()->objectStore('assets')
            ->put('logo.txt', 'hello', ['content-type' => 'text/plain'])->await();

        self::assertInstanceOf(ObjectInfo::class, $stored);
        self::assertSame('logo.txt', $stored->name);
        self::assertSame(5, $stored->size);
        self::assertSame(1, $stored->chunks);
        self::assertNotSame('', $stored->nuid);
        self::assertSame($this->digestOf('hello'), $stored->digest);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('PUB $O.assets.C.' . $stored->nuid . ' ', $writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('Nats-Rollup:sub', $writes);
    }

    /**
     * Verifies an overwrite purges the previous revision's chunk subject.
     */
    public function testPutOverwritePurgesPreviousChunks(): void
    {
        $oldNuid = 'oldnuid0001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // 1) existing-object lookup -> returns previous meta with old nuid
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // 2) chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // 3) meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
            // 4) purge old chunks ack
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->put('logo.txt', 'world')->await();

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * Verifies get downloads chunks via NUID subject and verifies the content digest.
     */
    public function testGetReturnsPayloadAndVerifiesDigest(): void
    {
        $nuid = 'nuidget0001';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPH1","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),                          // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),                  // create ephemeral consumer
            "MSG _INBOX.JS.FETCH.c 3 5\r\nhello\r\n",                                              // chunk delivery (no reply -> no ack)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),      // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
        self::assertSame('doc.txt', $fetched->info->name);
        self::assertSame($nuid, $fetched->info->nuid);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"filter_subject":"$O.assets.C.' . $nuid . '"', $writes);
    }

    /**
     * Verifies get throws on a content digest mismatch.
     */
    public function testGetThrowsOnDigestMismatch(): void
    {
        $nuid = 'nuidbad0001';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 11,
            'chunks' => 1,
            'digest' => $this->digestOf('hello world'),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPH2","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            "MSG _INBOX.JS.FETCH.c 3 11\r\nCORRUPTED!!\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies getToCallback streams the chunk payload to the handler.
     */
    public function testGetToCallbackStreamsChunks(): void
    {
        $nuid = 'nuidcb00001';
        $meta = $this->metaGetResponse('report.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPH3","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            "MSG _INBOX.JS.FETCH.c 3 5\r\nhello\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $captured = '';
        $calls = 0;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'report.txt',
            static function (string $chunk) use (&$captured, &$calls): void {
                $captured .= $chunk;
                $calls++;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertSame(1, $calls);
        self::assertNotNull($info);
        self::assertSame('report.txt', $info->name);
    }

    /**
     * Verifies getToCallback invokes the callback once per stored chunk (in order) for a multi-chunk
     * object, rather than buffering the whole object and calling back once.
     */
    public function testGetToCallbackInvokesCallbackOncePerChunk(): void
    {
        $nuid = 'nuidcbmulti';
        $chunks = ['abc', 'def', 'ghi'];
        $assembled = implode('', $chunks);
        $meta = $this->metaGetResponse('multi.txt', [
            'nuid' => $nuid,
            'size' => strlen($assembled),
            'chunks' => count($chunks),
            'digest' => $this->digestOf($assembled),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHCBM","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            "MSG _INBOX.JS.FETCH.c 3 3\r\nabc\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\ndef\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\nghi\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $captured = [];
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'multi.txt',
            static function (string $chunk) use (&$captured): void {
                $captured[] = $chunk;
            },
        )->await();

        // One invocation per stored chunk, in order — the whole object is never assembled.
        self::assertSame(['abc', 'def', 'ghi'], $captured);
        self::assertNotNull($info);
        self::assertSame('multi.txt', $info->name);
        self::assertSame(3, $info->chunks);
    }

    /**
     * Verifies getToCallback skips streaming for deleted objects.
     */
    public function testGetToCallbackSkipsDeletedObjects(): void
    {
        $meta = $this->metaGetResponse('gone.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $called = false;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'gone.txt',
            static function (string $chunk) use (&$called): void {
                $called = true;
            },
        )->await();

        self::assertFalse($called);
        self::assertNotNull($info);
        self::assertTrue($info->deleted);
    }

    /**
     * Verifies delete writes a tombstone via the encoded meta subject and purges chunks.
     */
    public function testDeleteWritesTombstoneAndPurgesChunks(): void
    {
        $oldNuid = 'delnuid0001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // 1) existing-object lookup
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // 2) tombstone meta publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),
            // 3) purge ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * Verifies list enumerates meta subjects and reads the latest record per object.
     */
    public function testListEnumeratesMetaSubjects(): void
    {
        $logoEnc = $this->encodeName('logo.txt');
        $oldEnc = $this->encodeName('old.txt');

        $streamInfo = json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => [
                'messages' => 3,
                'last_seq' => 3,
                'subjects' => [
                    '$O.assets.M.' . $logoEnc => 1,
                    '$O.assets.M.' . $oldEnc => 1,
                ],
            ],
        ], JSON_THROW_ON_ERROR);

        $logoMeta = $this->metaGetResponse('logo.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);
        $oldMeta = $this->metaGetResponse('old.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // list(): subjects enumeration, then last_by_subj per meta subject
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($logoMeta), $logoMeta),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($oldMeta), $oldMeta),
            // list(includeDeleted: true): same again
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($logoMeta), $logoMeta),
            sprintf("MSG _INBOX.f 6 %d\r\n%s\r\n", strlen($oldMeta), $oldMeta),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $active = $bucket->list()->await();
        $all = $bucket->list(includeDeleted: true)->await();

        self::assertCount(1, $active);
        self::assertSame('logo.txt', $active[0]->name);
        self::assertCount(2, $all);

        self::assertStringContainsString('"subjects_filter":"$O.assets.M.>"', $transport->writes[3]);
    }

    /**
     * Verifies info returns null when JetStream reports the object metadata is not found.
     */
    public function testInfoReturnsNullWhenNotFound(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        self::assertNull($client->jetStream()->objectStore('assets')->info('missing.txt')->await());
    }

    /**
     * Verifies watch parses metadata payloads and dispatches ObjectInfo updates.
     */
    public function testWatchDispatchesObjectInfo(): void
    {
        $enc = $this->encodeName('logo.txt');
        $metadata = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'wnuid1',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG \$O.assets.M.%s 1 %d\r\n%s\r\n", $enc, strlen((string) $metadata), (string) $metadata),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $sid = $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen = $info;
        })->await();

        self::assertSame(1, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(ObjectInfo::class, $seen);
        /** @var ObjectInfo $seenInfo */
        $seenInfo = $seen;
        self::assertSame('logo.txt', $seenInfo->name);
        self::assertSame('wnuid1', $seenInfo->nuid);
    }

    public function testBucketSubjectHelpers(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        self::assertSame('OBJ_assets', $bucket->streamName());
        self::assertSame('$O.assets.C.', $bucket->chunkPrefix());
        self::assertSame('$O.assets.M.', $bucket->metaPrefix());
    }

    public function testPutRejectsEmptyName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->put('', 'x')->await();
    }

    public function testPutSplitsIntoMultipleChunksWithSmallChunkSize(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // existing-object lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // three chunk acks
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // meta ack
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);
        $stored = $bucket->put('multi.bin', 'abcdefghij')->await();

        self::assertSame(3, $stored->chunks);
        $writes = implode('', $transport->writes);
        self::assertSame(3, substr_count($writes, 'PUB $O.assets.C.' . $stored->nuid));
        self::assertStringContainsString('"chunks":3', $writes);
    }

    /**
     * Verifies a multi-chunk object is downloaded with a single batched pull (not one pull per
     * chunk), reassembled in order, and digest-verified.
     */
    public function testGetDownloadsMultipleChunksInSingleBatch(): void
    {
        $nuid = 'nuidbatch01';
        $chunks = ['abc', 'def', 'ghi'];
        $assembled = implode('', $chunks);
        $meta = $this->metaGetResponse('multi.bin', [
            'nuid' => $nuid,
            'size' => strlen($assembled),
            'chunks' => count($chunks),
            'digest' => $this->digestOf($assembled),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHB","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),                     // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),             // create ephemeral consumer
            // All three chunks arrive on the single fetch-batch inbox (sid 3), no per-chunk pull.
            "MSG _INBOX.JS.FETCH.c 3 3\r\nabc\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\ndef\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\nghi\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),  // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('multi.bin')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame($assembled, $fetched->data);
        self::assertSame(count($chunks), $fetched->info->chunks);
        self::assertSame($this->digestOf($assembled), $fetched->info->digest);

        $writes = implode('||', $transport->writes);
        // One batched pull request for all chunks (batch == chunk count), not three separate pulls.
        self::assertStringContainsString('"filter_subject":"$O.assets.C.' . $nuid . '"', $writes);
        self::assertSame(1, substr_count($writes, 'PUB $JS.API.CONSUMER.MSG.NEXT.OBJ_assets'));
        self::assertStringContainsString('"batch":3', $writes);
    }

    /**
     * Verifies list() rethrows a non-404 error raised while reading a meta record.
     */
    public function testListRethrowsNonNotFoundError(): void
    {
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('doc.txt') => 1]],
        ], JSON_THROW_ON_ERROR);
        $error = '{"error":{"code":500,"description":"boom"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),  // metaSubjects()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($error), $error),            // last_by_subj -> error
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('boom');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    /**
     * Verifies list() surfaces an error from the meta-subject enumeration request.
     */
    public function testListThrowsWhenSubjectEnumerationFails(): void
    {
        $error = '{"error":{"code":500,"description":"info failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),  // metaSubjects() -> error
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('info failed');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    /**
     * Verifies delete() still succeeds when the best-effort chunk purge fails.
     */
    public function testDeleteToleratesPurgeFailure(): void
    {
        $meta = $this->metaGetResponse('logo.txt', ['nuid' => 'delnuidfail', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')]);
        $purgeError = '{"error":{"code":500,"description":"purge failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),                       // existing-object lookup
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),  // tombstone ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($purgeError), $purgeError),            // purge -> error (swallowed)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
    }

    /**
     * Verifies list() skips a meta subject whose record is no longer present (404).
     */
    public function testListSkipsNotFoundSubject(): void
    {
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => [
                '$O.assets.M.' . $this->encodeName('gone.txt') => 1,
                '$O.assets.M.' . $this->encodeName('logo.txt') => 1,
            ]],
        ], JSON_THROW_ON_ERROR);
        $logoMeta = $this->metaGetResponse('logo.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),               // metaSubjects()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),   // first subject -> 404 (skipped)
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($logoMeta), $logoMeta),                   // second subject -> present
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        self::assertCount(1, $objects);
        self::assertSame('logo.txt', $objects[0]->name);
    }

    /**
     * Verifies delete() proceeds when the previous-metadata lookup fails with a non-404 error.
     */
    public function testDeleteToleratesMissingPreviousMetadata(): void
    {
        $lookupError = '{"error":{"code":500,"description":"lookup failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($lookupError), $lookupError),              // lookup -> 500 (swallowed)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),        // tombstone ack
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
    }

    /**
     * Verifies delete() surfaces an error when the metadata (tombstone) publish is rejected.
     */
    public function testDeleteThrowsWhenMetadataPublishFails(): void
    {
        $publishError = '{"error":{"code":400,"description":"publish rejected"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),    // lookup -> 404 (no previous)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($publishError), $publishError),            // tombstone publish -> error
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('publish rejected');
        $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();
    }

    /**
     * Verifies a pull timeout (408) during download stops the loop, so the short read is caught by
     * the digest check rather than hanging.
     */
    public function testGetStopsOnPullTimeoutAndFailsDigest(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuidto01', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHTO","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 408 Request Timeout\r\nStatus: 408\r\nDescription: Request Timeout\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),                          // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),                  // create consumer
            sprintf("HMSG _INBOX.JS.FETCH.c 3 %d %d\r\n%s\r\n", $hb, $hb, $status),                 // pull -> 408 (break)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),       // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies a non-timeout pull error during download is propagated rather than swallowed.
     */
    public function testGetRethrowsNonTimeoutPullError(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuiderr01', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHERR","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            sprintf("HMSG _INBOX.JS.FETCH.c 3 %d %d\r\n%s\r\n", $hb, $hb, $status),                 // pull -> 409 (rethrow)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('status 409');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }
}
