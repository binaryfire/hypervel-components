<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\IntersectionTags;

use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;
use Redis;

/**
 * Flushes stale entries from intersection tag sorted sets.
 *
 * Uses ZREMRANGEBYSCORE to remove entries whose TTL timestamps have passed.
 * This is a cleanup operation that should be run periodically to prevent
 * memory leaks from expired cache key references.
 *
 * Entries with score -1 (forever items) are never flushed.
 */
class FlushStaleEntries
{
    public function __construct(
        private readonly StoreContext $context,
    ) {}

    /**
     * Flush stale entries from the given tag sorted sets.
     *
     * Removes entries with TTL scores between 0 and current timestamp.
     * Entries with score -1 (forever items) are not affected.
     *
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     */
    public function execute(array $tagIds): void
    {
        $this->context->withConnection(function (RedisConnection $conn) use ($tagIds) {
            $pipeline = $conn->multi(Redis::PIPELINE);
            $prefix = $this->context->prefix();

            foreach ($tagIds as $tagId) {
                $pipeline->zRemRangeByScore(
                    $prefix . $tagId,
                    '0',
                    (string) now()->getTimestamp()
                );
            }

            $pipeline->exec();
        });
    }
}
