<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Core\NatsHeaders;
use PHPUnit\Framework\TestCase;

final class NatsHeadersTest extends TestCase
{
    /**
     * Verifies a list header value emits one line per value (multimap encoding, #42).
     */
    public function testToWireBlockEmitsRepeatedLinesForListValue(): void
    {
        $raw = NatsHeaders::toWireBlock([
            'Link' => ['a.txt', 'b.txt'],
            'Nats-Msg-Id' => '1',
        ]);

        self::assertStringContainsString("Link:a.txt\r\n", $raw);
        self::assertStringContainsString("Link:b.txt\r\n", $raw);
        self::assertStringContainsString("Nats-Msg-Id:1\r\n", $raw);
    }

    /**
     * Verifies fromWireBlockMulti preserves every value of a repeated header, while fromWireBlock stays
     * last-value-wins (#42).
     */
    public function testFromWireBlockMultiPreservesAllValues(): void
    {
        $raw = NatsHeaders::toWireBlock(['Link' => ['a.txt', 'b.txt'], 'Single' => 'one']);

        $multi = NatsHeaders::fromWireBlockMulti($raw);
        self::assertSame(['a.txt', 'b.txt'], $multi['Link'] ?? null);
        self::assertSame(['one'], $multi['Single'] ?? null);

        $single = NatsHeaders::fromWireBlock($raw);
        self::assertSame('b.txt', $single['Link'] ?? null);
    }

    /**
     * Verifies fromWireBlockMulti parses the status line into single-element lists (#42).
     */
    public function testFromWireBlockMultiParsesStatusLine(): void
    {
        $multi = NatsHeaders::fromWireBlockMulti("NATS/1.0 503 No Responders\r\n\r\n");

        self::assertSame(['503'], $multi['Status'] ?? null);
        self::assertSame(['No Responders'], $multi['Description'] ?? null);
    }

    public function testRoundTripWireEncoding(): void
    {
        $raw = NatsHeaders::toWireBlock([
            'Nats-Msg-Id' => '123',
            'X-Trace' => 'abc',
        ]);

        self::assertStringStartsWith("NATS/1.0\r\n", $raw);
        self::assertStringEndsWith("\r\n\r\n", $raw);
        self::assertSame([
            'Nats-Msg-Id' => '123',
            'X-Trace' => 'abc',
        ], NatsHeaders::fromWireBlock($raw));
    }

    public function testFromWireBlockSkipsMalformedHeaderLines(): void
    {
        self::assertSame([], NatsHeaders::fromWireBlock(null));
        self::assertSame([], NatsHeaders::fromWireBlock(''));

        $raw = "NATS/1.0\r\n"
            . "NoSeparator\r\n"
            . ":missing-name\r\n"
            . "Valid: value\r\n"
            . "\r\n";

        self::assertSame(['Valid' => 'value'], NatsHeaders::fromWireBlock($raw));
    }

    public function testFromWireBlockParsesStatusLine(): void
    {
        $raw = "NATS/1.0 100 Idle Heartbeat\r\n"
            . "Nats-Consumer-Stalled: _INBOX.123\r\n"
            . "\r\n";

        self::assertSame([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Consumer-Stalled' => '_INBOX.123',
        ], NatsHeaders::fromWireBlock($raw));
    }

    public function testToWireBlockRejectsEmptyHeaderName(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Header name');
        NatsHeaders::toWireBlock(['' => 'value']);
    }

    public function testToWireBlockRejectsHeaderNameWithColonOrWhitespace(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Header name');
        NatsHeaders::toWireBlock(['a:b' => 'value']);
    }

    public function testHeaderValueSurroundingWhitespaceRoundTripsSymmetrically(): void
    {
        // Encode and decode both trim surrounding whitespace, so a value round-trips consistently
        // instead of asymmetrically losing leading/trailing spaces only on decode.
        $raw = NatsHeaders::toWireBlock(['X-Test' => '  spaced  ']);

        self::assertSame(['X-Test' => 'spaced'], NatsHeaders::fromWireBlock($raw));
    }
}
