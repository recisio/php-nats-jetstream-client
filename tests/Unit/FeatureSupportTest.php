<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\FeatureSupport;
use PHPUnit\Framework\TestCase;

final class FeatureSupportTest extends TestCase
{
    /**
     * Verifies the version registry returns the documented minimum versions (and null for unknowns).
     */
    public function testRequiredVersion(): void
    {
        self::assertSame('2.10', FeatureSupport::requiredVersion('filter_subjects'));
        self::assertSame('2.11', FeatureSupport::requiredVersion('allow_msg_ttl'));
        self::assertSame('2.12', FeatureSupport::requiredVersion('allow_atomic'));
        self::assertNull(FeatureSupport::requiredVersion('not_a_real_field'));
    }

    /**
     * Verifies an "unknown field" error for a version-gated feature maps to a typed exception that
     * carries the feature, required version, server version, and is catchable as a JetStreamException.
     */
    public function testUnsupportedFromApiErrorMapsKnownField(): void
    {
        $e = FeatureSupport::unsupportedFromApiError(
            'invalid JSON: json: unknown field "allow_atomic"',
            400,
            '2.10.5',
        );

        self::assertInstanceOf(UnsupportedFeatureException::class, $e);
        self::assertInstanceOf(JetStreamException::class, $e);
        self::assertSame('allow_atomic', $e->feature);
        self::assertSame('2.12', $e->requiredVersion);
        self::assertSame('2.10.5', $e->serverVersion);
        self::assertSame(400, $e->getCode());
        self::assertStringContainsString('requires NATS server 2.12+', $e->getMessage());
        self::assertStringContainsString('2.10.5', $e->getMessage());
    }

    /**
     * Verifies an unknown field that is not in the registry is left as an ordinary error (null).
     */
    public function testUnsupportedFromApiErrorIgnoresUnregisteredField(): void
    {
        self::assertNull(FeatureSupport::unsupportedFromApiError('unknown field "totally_made_up"', 400, '2.12.0'));
    }

    /**
     * Verifies a non-"unknown field" error is not treated as a feature gap.
     */
    public function testUnsupportedFromApiErrorIgnoresOtherErrors(): void
    {
        self::assertNull(FeatureSupport::unsupportedFromApiError('stream not found', 404, '2.12.0'));
    }

    /**
     * Verifies an unknown server version renders as "unknown" in the message.
     */
    public function testUnsupportedFromApiErrorWithUnknownServerVersion(): void
    {
        $e = FeatureSupport::unsupportedFromApiError('unknown field "allow_msg_ttl"', 400, null);

        self::assertInstanceOf(UnsupportedFeatureException::class, $e);
        self::assertNull($e->serverVersion);
        self::assertStringContainsString('reports unknown', $e->getMessage());
    }
}
