<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;

/**
 * Streaming parser for NATS protocol frames read from transport.
 */
final class ProtocolParser
{
    private string $buffer = '';

    /** Maximum total frame size (headers + payload) accepted from the server. */
    private int $maxFrameSize;

    /**
     * Parsed-but-incomplete MSG/HMSG header awaiting its payload bytes. Remembering it means a
     * large payload arriving across many chunks is not re-scanned/re-parsed on every push; each
     * subsequent push only checks whether enough bytes have accumulated.
     *
     * @var array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}|null
     */
    private ?array $pending = null;

    /**
     * Creates a parser for line and payload frames produced by the NATS server.
     *
     * @param int $maxFrameSize Maximum total MSG/HMSG bytes accepted per frame to limit memory usage.
     */
    public function __construct(int $maxFrameSize = 8 * 1024 * 1024)
    {
        $this->maxFrameSize = $maxFrameSize;
    }

    /**
     * Appends a raw socket chunk and emits all complete frames currently available.
     *
     * @return list<ProtocolFrame>
     */
    public function push(string $chunk): array
    {
        if ($chunk !== '') {
            $this->buffer .= $chunk;
        }

        $frames = [];
        $offset = 0;
        $bufferLength = strlen($this->buffer);

        while ($offset < $bufferLength) {
            if ($this->pending !== null) {
                $required = $this->pending['payloadOffset'] + $this->pending['totalBytes'] + 2;
                if ($bufferLength < $required) {
                    // Payload still incomplete; keep buffered bytes for the next chunk without
                    // re-scanning or re-parsing the control line.
                    break;
                }

                $frames[] = $this->buildPendingFrame();
                $offset = $required;
                $this->pending = null;

                continue;
            }

            $lineEndPos = strpos($this->buffer, "\r\n", $offset);
            if ($lineEndPos === false) {
                break;
            }

            $line = substr($this->buffer, $offset, $lineEndPos - $offset);
            $nextOffset = $lineEndPos + 2;

            if (str_starts_with($line, 'MSG ') || str_starts_with($line, 'HMSG ')) {
                // Parse the control line once; the next loop iteration (or a later push) completes
                // the payload. Header validation/size limits are enforced here immediately.
                $this->pending = $this->parseDataFrameHeader($line, $nextOffset);

                continue;
            }

            $frames[] = $this->parseControlFrame($line);
            $offset = $nextOffset;
        }

        if ($offset > 0) {
            $this->buffer = substr($this->buffer, $offset);

            // Keep the pending payload offset relative to the trimmed buffer.
            if ($this->pending !== null) {
                $this->pending['payloadOffset'] -= $offset;
            }
        }

        return $frames;
    }

    /**
     * Parses line-based control frames that do not carry trailing payload bytes.
     */
    private function parseControlFrame(string $line): ProtocolFrame
    {
        if (str_starts_with($line, 'INFO ')) {
            return new ProtocolFrame(
                type: ProtocolFrameType::Info,
                infoPayload: substr($line, 5),
            );
        }

        if ($line === 'PING') {
            return new ProtocolFrame(type: ProtocolFrameType::Ping);
        }

        if ($line === 'PONG') {
            return new ProtocolFrame(type: ProtocolFrameType::Pong);
        }

        if ($line === '+OK') {
            return new ProtocolFrame(type: ProtocolFrameType::Ok);
        }

        if (str_starts_with($line, '-ERR')) {
            return new ProtocolFrame(
                type: ProtocolFrameType::Err,
                error: trim(substr($line, 4)),
            );
        }

        throw new ProtocolException('Unsupported control frame: ' . $line);
    }

    /**
     * Parses a MSG or HMSG control line into a pending-frame descriptor and validates its size.
     *
     * @param int $payloadOffset Absolute offset into the current buffer where payload bytes begin.
     * @return array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}
     */
    private function parseDataFrameHeader(string $line, int $payloadOffset): array
    {
        if (str_starts_with($line, 'HMSG ')) {
            return $this->parseHMsgHeader($line, $payloadOffset);
        }

        return $this->parseMsgHeader($line, $payloadOffset);
    }

    /**
     * @return array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}
     */
    private function parseMsgHeader(string $line, int $payloadOffset): array
    {
        $parts = preg_split('/\s+/', $line);
        if ($parts === false || count($parts) < 4 || count($parts) > 5) {
            throw new ProtocolException('Invalid MSG frame line: ' . $line);
        }

        $size = (int) $parts[count($parts) - 1];
        if ($size < 0 || $size > $this->maxFrameSize) {
            throw new ProtocolException('MSG frame payload size is invalid: ' . $size);
        }

        return [
            'type' => ProtocolFrameType::Msg,
            'subject' => $parts[1],
            'sid' => (int) $parts[2],
            'replyTo' => count($parts) === 5 ? $parts[3] : null,
            'headerBytes' => null,
            'totalBytes' => $size,
            'payloadOffset' => $payloadOffset,
        ];
    }

    /**
     * @return array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}
     */
    private function parseHMsgHeader(string $line, int $payloadOffset): array
    {
        $parts = preg_split('/\s+/', $line);
        if ($parts === false || count($parts) < 5 || count($parts) > 6) {
            throw new ProtocolException('Invalid HMSG frame line: ' . $line);
        }

        if (count($parts) === 6) {
            $replyTo = $parts[3];
            $headerBytes = (int) $parts[4];
            $totalBytes = (int) $parts[5];
        } else {
            $replyTo = null;
            $headerBytes = (int) $parts[3];
            $totalBytes = (int) $parts[4];
        }

        if ($totalBytes < 0 || $totalBytes > $this->maxFrameSize) {
            throw new ProtocolException('HMSG frame payload size is invalid: ' . $totalBytes);
        }

        return [
            'type' => ProtocolFrameType::HMsg,
            'subject' => $parts[1],
            'sid' => (int) $parts[2],
            'replyTo' => $replyTo,
            'headerBytes' => $headerBytes,
            'totalBytes' => $totalBytes,
            'payloadOffset' => $payloadOffset,
        ];
    }

    /**
     * Builds the frame for the currently buffered pending header once its payload is complete.
     */
    private function buildPendingFrame(): ProtocolFrame
    {
        /** @var array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int} $pending */
        $pending = $this->pending;

        $payload = substr($this->buffer, $pending['payloadOffset'], $pending['totalBytes']);
        $crlf = substr($this->buffer, $pending['payloadOffset'] + $pending['totalBytes'], 2);

        if ($crlf !== "\r\n") {
            $label = $pending['type'] === ProtocolFrameType::HMsg ? 'HMSG' : 'MSG';
            throw new ProtocolException($label . ' frame payload must be terminated by CRLF');
        }

        return new ProtocolFrame(
            type: $pending['type'],
            subject: $pending['subject'],
            sid: $pending['sid'],
            replyTo: $pending['replyTo'],
            payload: $payload,
            headerBytes: $pending['headerBytes'],
            totalBytes: $pending['type'] === ProtocolFrameType::HMsg ? $pending['totalBytes'] : null,
        );
    }
}
