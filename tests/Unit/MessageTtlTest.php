<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\MessageTtl;
use PHPUnit\Framework\TestCase;

final class MessageTtlTest extends TestCase
{
    /**
     * Verifies an integer number of seconds is rendered as a seconds duration.
     */
    public function testFormatsIntegerSeconds(): void
    {
        self::assertSame('30s', MessageTtl::format(30));
    }

    /**
     * Verifies a bare integer string is also treated as seconds.
     */
    public function testFormatsIntegerStringAsSeconds(): void
    {
        self::assertSame('45s', MessageTtl::format('45'));
    }

    /**
     * Verifies a Go duration string passes through unchanged.
     */
    public function testFormatsDurationStringUnchanged(): void
    {
        self::assertSame('1h30m', MessageTtl::format('1h30m'));
    }

    /**
     * Verifies "never" passes through unchanged.
     */
    public function testFormatsNever(): void
    {
        self::assertSame('never', MessageTtl::format('never'));
    }

    /**
     * Verifies a zero / sub-second integer TTL is rejected.
     */
    public function testRejectsZeroSeconds(): void
    {
        $this->expectException(JetStreamException::class);

        MessageTtl::format(0);
    }

    /**
     * Verifies a negative TTL is rejected.
     */
    public function testRejectsNegativeSeconds(): void
    {
        $this->expectException(JetStreamException::class);

        MessageTtl::format(-5);
    }

    /**
     * Verifies an empty string TTL is rejected.
     */
    public function testRejectsEmptyString(): void
    {
        $this->expectException(JetStreamException::class);

        MessageTtl::format('   ');
    }
}
