<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use PHPUnit\Framework\TestCase;

final class WebSocketFrameCodecTest extends TestCase
{
    /**
     * Verifies a masked client frame round-trips through decode() (#31).
     */
    public function testEncodeMaskedFrameRoundTrips(): void
    {
        $encoded = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, "PING\r\n", true, 'ABCD');

        // FIN+binary, mask bit set, length 6.
        self::assertSame(0x82, ord($encoded[0]));
        self::assertSame(0x80 | 6, ord($encoded[1]));

        $buffer = $encoded;
        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertSame('', $buffer);
        self::assertCount(1, $frames);
        self::assertSame(WebSocketFrameCodec::OP_BINARY, $frames[0]['opcode']);
        self::assertSame("PING\r\n", $frames[0]['payload']);
        self::assertTrue($frames[0]['fin']);
    }

    /**
     * Verifies the RFC 6455 §1.3 Sec-WebSocket-Accept example vector (#31).
     */
    public function testAcceptKeyMatchesRfcExample(): void
    {
        self::assertSame(
            's3pPLMBiTxaQ9kYGzzhZRbK+xOo=',
            WebSocketFrameCodec::acceptKey('dGhlIHNhbXBsZSBub25jZQ=='),
        );
    }

    /**
     * Verifies decode() leaves an incomplete trailing frame in the buffer (#31).
     */
    public function testDecodeKeepsIncompleteTrailingFrame(): void
    {
        $full = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hello', true, 'WXYZ');
        // Feed everything but the last byte.
        $buffer = substr($full, 0, -1);
        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertSame([], $frames);
        self::assertSame(substr($full, 0, -1), $buffer);

        // Deliver the final byte; the frame now decodes.
        $buffer .= substr($full, -1);
        $frames = WebSocketFrameCodec::decode($buffer);
        self::assertCount(1, $frames);
        self::assertSame('hello', $frames[0]['payload']);
        self::assertSame('', $buffer);
    }

    /**
     * Verifies decode() returns multiple frames present in one buffer (#31).
     */
    public function testDecodeReturnsMultipleFrames(): void
    {
        $buffer = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'INFO {}', false)
            . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'hb', false)
            . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, '+OK', false);

        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertCount(3, $frames);
        self::assertSame('INFO {}', $frames[0]['payload']);
        self::assertSame(WebSocketFrameCodec::OP_PING, $frames[1]['opcode']);
        self::assertSame('+OK', $frames[2]['payload']);
    }

    /**
     * Verifies an extended (16-bit length) unmasked server frame decodes (#31).
     */
    public function testDecodeExtended16BitLengthFrame(): void
    {
        $payload = str_repeat('x', 300);
        $buffer = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

        // Length marker 126 + 2-byte length.
        self::assertSame(126, ord($buffer[1]) & 0x7F);

        $frames = WebSocketFrameCodec::decode($buffer);
        self::assertCount(1, $frames);
        self::assertSame($payload, $frames[0]['payload']);
    }

    /**
     * Verifies a fragmented (FIN=0) data frame is reported with fin=false (#31).
     */
    public function testDecodeReportsFragmentationFlag(): void
    {
        // Manually craft a non-final binary frame (FIN=0, opcode binary, unmasked, len 3).
        $buffer = chr(WebSocketFrameCodec::OP_BINARY) . chr(3) . 'abc';
        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertCount(1, $frames);
        self::assertFalse($frames[0]['fin']);
        self::assertSame('abc', $frames[0]['payload']);
    }

    /**
     * Verifies permessage-deflate deflate/inflate round-trips (#61).
     */
    public function testDeflateInflateRoundTrip(): void
    {
        $payload = str_repeat('NATS-over-websocket payload ', 20);

        $compressed = WebSocketFrameCodec::deflate($payload);
        self::assertNotSame($payload, $compressed);
        self::assertLessThan(strlen($payload), strlen($compressed));
        self::assertSame($payload, WebSocketFrameCodec::inflate($compressed));
    }

    /**
     * Verifies a compressed frame carries RSV1 and decodes back to the original payload (#61).
     */
    public function testCompressedFrameRoundTrip(): void
    {
        $payload = 'PUB orders.created 5\r\nhello\r\n';
        $encoded = WebSocketFrameCodec::encode(
            WebSocketFrameCodec::OP_BINARY,
            WebSocketFrameCodec::deflate($payload),
            true,
            'MASK',
            true,
        );

        // RSV1 bit set on the first byte.
        self::assertSame(0x40, ord($encoded[0]) & 0x40);

        $buffer = $encoded;
        $frames = WebSocketFrameCodec::decode($buffer);
        self::assertCount(1, $frames);
        self::assertTrue($frames[0]['rsv1']);
        self::assertSame($payload, WebSocketFrameCodec::inflate($frames[0]['payload']));
    }

    /**
     * Verifies a non-4-byte mask key is rejected by encode() (#31).
     */
    public function testEncodeRejectsBadMaskKey(): void
    {
        $this->expectException(ProtocolException::class);
        WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'x', true, 'AB');
    }
}
