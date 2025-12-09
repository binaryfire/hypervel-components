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
     * In cluster mode, uses sequential commands since RedisCluster
     * doesn't support pipeline mode and tags may be in different slots.
     *
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     */
    public function execute(array $tagIds): void
    {
        if (empty($tagIds)) {
            return;
        }

        // Cluster mode: RedisCluster doesn't support pipeline, and tags
        // may be in different slots requiring sequential commands
        if ($this->context->isCluster()) {
            $this->executeCluster($tagIds);
            return;
        }

        $this->executePipeline($tagIds);
    }

    /**
     * Execute using pipeline for standard Redis (non-cluster).
     */
    private function executePipeline(array $tagIds): void
    {
        $this->context->withConnection(function (RedisConnection $conn) use ($tagIds) {
            $pipeline = $conn->multi(Redis::PIPELINE);
            $prefix = $this->context->prefix();
            $timestamp = (string) now()->getTimestamp();

            foreach ($tagIds as $tagId) {
                $pipeline->zRemRangeByScore(
                    $prefix . $tagId,
                    '0',
                    $timestamp
                );
            }

            $pipeline->exec();
        });
    }

    /**
     * Execute using sequential commands for Redis Cluster.
     *
     * Each tag sorted set may be in a different slot, so we must
     * execute ZREMRANGEBYSCORE commands sequentially rather than in a pipeline.
     */
    private function executeCluster(array $tagIds): void
    {
        $this->context->withConnection(function (RedisConnection $conn) use ($tagIds) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $timestamp = (string) now()->getTimestamp();

            foreach ($tagIds as $tagId) {
                $client->zRemRangeByScore(
                    $prefix . $tagId,
                    '0',
                    $timestamp
                );
            }
        });
    }
}
