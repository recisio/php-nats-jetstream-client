<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use IDCT\NATS\Exception\ProtocolException;

/**
 * Minimal RFC 6455 WebSocket frame codec for the NATS WebSocket transport.
 *
 * NATS-over-WebSocket carries raw NATS protocol bytes as WebSocket binary frame payloads. This codec
 * encodes masked client frames (clients MUST mask), decodes server frames (which are unmasked),
 * reassembles fragmented messages, and surfaces control frames (ping/pong/close) to the caller. It is
 * deliberately self-contained (no HTTP/WebSocket dependency) and pure, so it is fully unit-testable.
 */
final class WebSocketFrameCodec
{
    public const OP_CONTINUATION = 0x0;
    public const OP_TEXT = 0x1;
    public const OP_BINARY = 0x2;
    public const OP_CLOSE = 0x8;
    public const OP_PING = 0x9;
    public const OP_PONG = 0xA;

    /** GUID appended to the client key when computing the server's accept value (RFC 6455 §1.3). */
    private const HANDSHAKE_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    /** Hard cap on a single frame's declared payload length, to bound memory on a hostile/garbled stream. */
    private const MAX_FRAME_PAYLOAD = 64 * 1024 * 1024;

    /**
     * Encodes a single final ({@code FIN=1}) frame. When $mask is true (the default — required for
     * client→server frames) the payload is masked with a fresh 4-byte key.
     *
     * @param string|null $maskKey Optional fixed mask key (4 bytes) for deterministic tests; otherwise random.
     */
    public static function encode(int $opcode, string $payload, bool $mask = true, ?string $maskKey = null): string
    {
        $length = strlen($payload);
        $frame = pack('C', 0x80 | ($opcode & 0x0F)); // FIN=1 + opcode

        $maskBit = $mask ? 0x80 : 0x00;
        if ($length <= 125) {
            $frame .= pack('C', $maskBit | $length);
        } elseif ($length <= 0xFFFF) {
            $frame .= pack('C', $maskBit | 126) . pack('n', $length);
        } else {
            // 64-bit length. pack('J') needs PHP 64-bit ints, which this library already requires.
            $frame .= pack('C', $maskBit | 127) . pack('J', $length);
        }

        if (!$mask) {
            return $frame . $payload;
        }

        $key = $maskKey ?? random_bytes(4);
        if (strlen($key) !== 4) {
            throw new ProtocolException('WebSocket mask key must be exactly 4 bytes');
        }

        return $frame . $key . ($payload === '' ? '' : (string) (self::applyMask($payload, $key)));
    }

    /**
     * Decodes as many complete frames as are present in $buffer, removing the consumed bytes (an
     * incomplete trailing frame is left in $buffer for the next read).
     *
     * @return list<array{opcode:int,payload:string,fin:bool}>
     */
    public static function decode(string &$buffer): array
    {
        $frames = [];

        while (true) {
            $available = strlen($buffer);
            if ($available < 2) {
                break;
            }

            $byte1 = ord($buffer[0]);
            $byte2 = ord($buffer[1]);
            $fin = ($byte1 & 0x80) !== 0;
            $opcode = $byte1 & 0x0F;
            $masked = ($byte2 & 0x80) !== 0;
            $length = $byte2 & 0x7F;

            $offset = 2;
            if ($length === 126) {
                if ($available < $offset + 2) {
                    break;
                }
                /** @var array{1:int} $unpacked */
                $unpacked = unpack('n', substr($buffer, $offset, 2));
                $length = $unpacked[1];
                $offset += 2;
            } elseif ($length === 127) {
                if ($available < $offset + 8) {
                    break;
                }
                /** @var array{1:int} $unpacked */
                $unpacked = unpack('J', substr($buffer, $offset, 8));
                $length = $unpacked[1];
                $offset += 8;
            }

            if ($length < 0 || $length > self::MAX_FRAME_PAYLOAD) {
                throw new ProtocolException('WebSocket frame payload length out of bounds: ' . $length);
            }

            $maskKey = '';
            if ($masked) {
                if ($available < $offset + 4) {
                    break;
                }
                $maskKey = substr($buffer, $offset, 4);
                $offset += 4;
            }

            if ($available < $offset + $length) {
                // Full payload not yet buffered; wait for more bytes.
                break;
            }

            $payload = $length > 0 ? substr($buffer, $offset, $length) : '';
            if ($masked && $payload !== '') {
                $payload = (string) self::applyMask($payload, $maskKey);
            }

            $buffer = substr($buffer, $offset + $length);
            $frames[] = ['opcode' => $opcode, 'payload' => $payload, 'fin' => $fin];
        }

        return $frames;
    }

    /**
     * Computes the value the server must return in `Sec-WebSocket-Accept` for a given client key.
     */
    public static function acceptKey(string $clientKey): string
    {
        return base64_encode(sha1($clientKey . self::HANDSHAKE_GUID, true));
    }

    /**
     * Generates a fresh base64 client key for `Sec-WebSocket-Key` (16 random bytes).
     */
    public static function generateClientKey(): string
    {
        return base64_encode(random_bytes(16));
    }

    /**
     * XORs $payload with the repeating 4-byte $key (masking is its own inverse).
     */
    private static function applyMask(string $payload, string $key): string
    {
        $masked = $payload ^ str_repeat($key, intdiv(strlen($payload), 4) + 1);

        return substr($masked, 0, strlen($payload));
    }
}
