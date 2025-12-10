<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor;

use Hypervel\Cache\Contracts\Repository;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;

/**
 * Context object holding shared state for Doctor checks.
 *
 * Bundles all dependencies needed by functional checks to avoid
 * passing many parameters to each check's run() method.
 */
final class DoctorContext
{
    /**
     * Unique prefix to prevent collision with production data.
     */
    private const TEST_PREFIX = '_erc:doctor:';

    /**
     * Create a new doctor context instance.
     */
    public function __construct(
        public readonly Repository $cache,
        public readonly RedisStore $store,
        public readonly RedisConnection $redis,
        public readonly string $cachePrefix,
        public readonly string $storeName,
    ) {}

    /**
     * Get a value prefixed with the unique doctor test prefix.
     * Used for both cache keys and tag names to ensure complete isolation from production data.
     */
    public function prefixed(string $value): string
    {
        return self::TEST_PREFIX . $value;
    }

    /**
     * Get the full Redis key for a tag hash.
     */
    public function tagHashKey(string $tag): string
    {
        return $this->store->getContext()->tagHashKey($tag);
    }

    /**
     * Get the test prefix constant for cleanup operations.
     */
    public function getTestPrefix(): string
    {
        return self::TEST_PREFIX;
    }

    /**
     * Check if the store is configured for union tagging mode.
     */
    public function isUnionMode(): bool
    {
        return $this->store->getTaggingMode() === 'union';
    }

    /**
     * Check if the store is configured for intersection tagging mode.
     */
    public function isIntersectionMode(): bool
    {
        return $this->store->getTaggingMode() === 'intersection';
    }

    /**
     * Get the current tagging mode.
     */
    public function getTaggingMode(): string
    {
        return $this->store->getTaggingMode();
    }
}
