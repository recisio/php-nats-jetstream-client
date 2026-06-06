<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class KeyValueBucketTest extends TestCase
{
    /**
     * Verifies KV bucket create/delete map to KV stream lifecycle APIs.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $created = $kv->create()->await();
        $deleted = $kv->deleteBucket()->await();

        self::assertSame('KV_cfg', $created->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.KV_cfg', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.KV_cfg', $transport->writes[6]);
    }

    /**
     * Verifies KV put/get/delete operations map and parse values correctly.
     */
    public function testPutGetDelete(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';
        $getPayload = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":1,"data":"%s"}}',
            base64_encode('blue'),
        );
        $deleteAck = '{"stream":"KV_cfg","seq":2,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($getPayload), $getPayload),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($deleteAck), $deleteAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $put = $kv->put('theme', 'blue')->await();
        $entry = $kv->get('theme')->await();
        $delete = $kv->delete('theme')->await();

        self::assertSame('KV_cfg', $put->stream);
        self::assertInstanceOf(KeyValueEntry::class, $entry);
        self::assertSame('theme', $entry->key);
        self::assertSame('blue', $entry->value);
        self::assertSame('PUT', $entry->operation);
        self::assertSame('KV_cfg', $delete->stream);

        self::assertStringStartsWith('PUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringStartsWith('PUB $JS.API.STREAM.MSG.GET.KV_cfg _INBOX.', $transport->writes[6]);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[9]);
        self::assertStringContainsString('KV-Operation:DEL', $transport->writes[9]);
    }

    /**
     * Verifies get() returns null for missing keys.
     */
    public function testGetMissingReturnsNull(): void
    {
        $missingPayload = '{"error":{"code":404,"description":"message not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($missingPayload), $missingPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('missing')->await();

        self::assertNull($entry);
    }

    /**
     * Verifies invalid KV keys are rejected.
     */
    public function testInvalidKeyRejected(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');

        $client->jetStream()->keyValue('cfg')->put('a b', 'x')->await();
    }

    /**
     * Verifies optimistic update sends expected revision header.
     */
    public function testUpdateWithExpectedRevision(): void
    {
        $updateAck = '{"stream":"KV_cfg","seq":3,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($updateAck), $updateAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->update('theme', 'green', 2)->await();

        self::assertSame(3, $ack->seq);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:2', $transport->writes[3]);
    }

    /**
     * Verifies purge sends KV-Operation PURGE and Nats-Rollup headers.
     */
    public function testPurge(): void
    {
        $purgeAck = '{"stream":"KV_cfg","seq":4,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($purgeAck), $purgeAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->purge('theme')->await();

        self::assertSame(4, $ack->seq);
        self::assertStringContainsString('KV-Operation:PURGE', $transport->writes[3]);
        self::assertStringContainsString('Nats-Rollup:sub', $transport->writes[3]);
    }

    /**
     * Verifies getStatus maps stream state counters.
     */
    public function testGetStatus(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":7,"bytes":128,"subjects":{"$KV.cfg.theme":3}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $status = $client->jetStream()->keyValue('cfg')->getStatus()->await();

        self::assertSame('cfg', $status['bucket']);
        self::assertSame('KV_cfg', $status['stream']);
        self::assertSame(7, $status['messages']);
        self::assertSame(128, $status['bytes']);
    }

    /**
     * Verifies getAll returns only the latest non-deleted values by key.
     */
    public function testGetAll(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":4,"bytes":256,"subjects":{"$KV.cfg.username":2,"$KV.cfg.email":2}}}';
        // getAll() reads each key concurrently via Direct Get: subjects enumerate as [username, email]
        // (sids 2, 3). Direct Get returns the raw value as the body with Nats-* + KV-Operation headers.
        $usernameHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.username\r\nNats-Sequence: 3\r\nKV-Operation: PURGE\r\n\r\n";
        $emailHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.email\r\nNats-Sequence: 4\r\n\r\n";
        $emailBody = 'b@example.com';
        $uh = strlen($usernameHdrs);
        $eh = strlen($emailHdrs);
        $et = $eh + strlen($emailBody);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),       // STREAM.INFO subjects
            sprintf("HMSG _INBOX.b 2 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),            // username -> PURGE (skipped)
            sprintf("HMSG _INBOX.c 3 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody), // email -> value
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['email' => 'b@example.com'], $all);
        self::assertStringContainsString('$JS.API.DIRECT.GET.KV_cfg', implode('', $transport->writes));
    }

    // ─── Key Validation ─────────────────────────────────────────────

    public function testPutAcceptsKeyWithDotsColonsSlashes(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->put('config/v2:main.yaml', 'data')->await();
        self::assertSame(1, $ack->seq);
    }

    public function testPutRejectsKeyWithWildcard(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put('foo*bar', 'data')->await();
    }

    public function testPutRejectsKeyWithLeadingTrailingOrConsecutiveDots(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $kv = $client->jetStream()->keyValue('cfg');

        // Leading/trailing/consecutive dots make an empty subject token; all must be rejected.
        // (Dots, colons and slashes elsewhere remain valid — see testPutAcceptsKeyWithDotsColonsSlashes.)
        foreach (['.theme', 'theme.', 'a..b'] as $key) {
            try {
                $kv->put($key, 'data')->await();
                self::fail("Expected rejection for malformed key: {$key}");
            } catch (JetStreamException $e) {
                self::assertStringContainsString('Invalid KV key', $e->getMessage());
            }
        }
    }

    public function testPutRejectsKeyWithTab(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put("foo\tbar", 'data')->await();
    }

    // ─── KV Options Mapping ─────────────────────────────────────────

    public function testCreateWithSemanticOptions(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->create([
            'history' => 5,
            'ttl' => 86400000000000,
            'max_value_size' => 1024,
            'storage' => 'memory',
            'num_replicas' => 3,
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"max_msgs_per_subject":5', $written);
        self::assertStringContainsString('"max_age":86400000000000', $written);
        self::assertStringContainsString('"max_msg_size":1024', $written);
        self::assertStringContainsString('"storage":"memory"', $written);
        self::assertStringContainsString('"num_replicas":3', $written);
    }

    /**
     * Verifies watch callback receives KV entries from subscription dispatch.
     */
    public function testWatchDispatchesEntries(): void
    {
        // watch() now runs over a JetStream push consumer: create the consumer (request sid 1), then
        // the update is delivered on the deliver inbox (sid 2) carrying its stream sequence in the
        // $JS.ACK reply, which becomes the entry revision.
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $sid = $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry) use (&$seen): void {
            $seen = $entry;
        })->await();

        self::assertSame(2, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(KeyValueEntry::class, $seen);
        /** @var KeyValueEntry $seenEntry */
        $seenEntry = $seen;
        self::assertSame('theme', $seenEntry->key);
        self::assertSame('blue', $seenEntry->value);
        // The revision is now populated from the delivery's stream sequence (was always null before).
        self::assertSame(7, $seenEntry->revision);

        $createRequest = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"new"', $createRequest);
        self::assertStringContainsString('"ack_policy":"none"', $createRequest);
    }

    /**
     * Verifies non-404 API errors are propagated by get().
     */
    public function testGetPropagatesNon404ApiErrors(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"internal error"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('internal error');

        $client->jetStream()->keyValue('cfg')->get('theme')->await();
    }

    public function testGetWrapsMalformedReplyAsJetStreamException(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.a 1 7\r\nnotjson\r\n", // a non-JSON reply
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // A malformed reply must surface as the library's JetStreamException, not a raw \JsonException.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream reply');

        $client->jetStream()->keyValue('cfg')->get('theme')->await();
    }

    /**
     * Verifies DEL marker headers are mapped to tombstone entry values.
     */
    public function testGetMapsDeleteMarkerToNullValue(): void
    {
        $headers = base64_encode("NATS/1.0\r\nKV-Operation:DEL\r\n\r\n");
        $payload = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":3,"data":"%s","hdrs":"%s"}}',
            base64_encode('ignored'),
            $headers,
        );

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($payload), $payload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        self::assertSame('DEL', $entry->operation);
        self::assertNull($entry->value);
    }

    public function testBucketNameHelpers(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        self::assertSame('KV_cfg', $kv->streamName());
        self::assertSame('$KV.cfg.', $kv->subjectPrefix());
    }

    public function testUpdateRejectsNonPositiveExpectedRevision(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Expected revision must be greater than zero');
        $client->jetStream()->keyValue('cfg')->update('theme', 'v', 0)->await();
    }

    public function testGetRejectsMalformedBase64Payload(): void
    {
        $payload = '{"message":{"subject":"$KV.cfg.theme","seq":6,"data":"%%%not-base64%%%"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($payload), $payload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed KV payload for key theme');

        $client->jetStream()->keyValue('cfg')->get('theme')->await();
    }

    public function testGetAllSkipsKeysThatReturnNotFound(): void
    {
        // A key whose Direct Get races a deletion/expiry returns 404; getAll must skip it, not fail.
        $streamInfo = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":2,"bytes":64,"subjects":{"$KV.cfg.gone":1,"$KV.cfg.theme":1}}}';
        $notFound = "NATS/1.0 404 Message Not Found\r\nStatus: 404\r\n\r\n";
        $themeHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.theme\r\nNats-Sequence: 7\r\n\r\n";
        $themeBody = 'dark';
        $nf = strlen($notFound);
        $th = strlen($themeHdrs);
        $tt = $th + strlen($themeBody);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),       // STREAM.INFO subjects
            sprintf("HMSG _INBOX.b 2 %d %d\r\n%s\r\n", $nf, $nf, $notFound),                // gone -> 404 (skipped)
            sprintf("HMSG _INBOX.c 3 %d %d\r\n%s%s\r\n", $th, $tt, $themeHdrs, $themeBody), // theme -> value
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['theme' => 'dark'], $all);
    }

    public function testGetStatusFallsBackLastSequenceToMessagesWhenMissing(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg"},"state":{"messages":11,"bytes":128}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $status = $client->jetStream()->keyValue('cfg')->getStatus()->await();

        self::assertSame(11, $status['messages']);
        self::assertSame(11, $status['last_sequence']);
    }

    public function testDeletePropagatesApiError(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"delete failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('delete failed');

        $client->jetStream()->keyValue('cfg')->delete('theme')->await();
    }
}
