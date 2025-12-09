<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\IntersectionTags;

use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Adds a cache key reference to intersection tag sorted sets.
 *
 * Each tag maintains a sorted set where:
 * - Members are cache keys
 * - Scores are TTL timestamps (or -1 for forever items)
 *
 * This allows efficient tag-based cache invalidation and cleanup
 * of expired entries via ZREMRANGEBYSCORE.
 */
class AddEntry
{
    public function __construct(
        private readonly StoreContext $context,
    ) {}

    /**
     * Add a cache key entry to tag sorted sets.
     *
     * @param string $key The cache key (without prefix)
     * @param int $ttl TTL in seconds (0 means forever, stored as -1 score)
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     * @param string|null $updateWhen Optional ZADD flag: 'NX' (only add new), 'XX' (only update existing), 'GT'/'LT'
     */
    public function execute(string $key, int $ttl, array $tagIds, ?string $updateWhen = null): void
    {
        // Convert TTL to timestamp score:
        // - If TTL > 0: timestamp when this entry expires
        // - If TTL <= 0: -1 to indicate "forever" (won't be cleaned by ZREMRANGEBYSCORE)
        $score = $ttl > 0 ? now()->addSeconds($ttl)->getTimestamp() : -1;

        $this->context->withConnection(function (RedisConnection $conn) use ($key, $score, $tagIds, $updateWhen) {
            $prefix = $this->context->prefix();

            foreach ($tagIds as $tagId) {
                $prefixedTagKey = $prefix . $tagId;

                if ($updateWhen) {
                    // ZADD with flag (NX, XX, GT, LT)
                    $conn->zadd($prefixedTagKey, $updateWhen, $score, $key);
                } else {
                    // Standard ZADD
                    $conn->zadd($prefixedTagKey, $score, $key);
                }
            }
        });
    }
}
