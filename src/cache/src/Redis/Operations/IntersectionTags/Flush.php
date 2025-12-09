<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\IntersectionTags;

use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Flushes all cache entries associated with intersection tags.
 *
 * This operation:
 * 1. Gets all cache keys from the tag sorted sets
 * 2. Deletes the cache keys in chunks (1000 at a time)
 * 3. Deletes the tag sorted sets themselves
 */
class Flush
{
    private const CHUNK_SIZE = 1000;

    public function __construct(
        private readonly StoreContext $context,
        private readonly GetEntries $getEntries,
    ) {}

    /**
     * Flush all cache entries for the given tags.
     *
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     * @param array<string> $tagNames Array of tag names (e.g., ["users", "posts"])
     */
    public function execute(array $tagIds, array $tagNames): void
    {
        $this->flushValues($tagIds);
        $this->flushTags($tagNames);
    }

    /**
     * Flush the individual cache entries for the tags.
     *
     * @param array<string> $tagIds Array of tag identifiers
     */
    private function flushValues(array $tagIds): void
    {
        $prefix = $this->context->prefix();

        $entries = $this->getEntries->execute($tagIds)
            ->map(fn (string $key) => $prefix . $key)
            ->chunk(self::CHUNK_SIZE);

        foreach ($entries as $cacheKeys) {
            $this->context->withConnection(function (RedisConnection $conn) use ($cacheKeys) {
                $conn->del(...$cacheKeys);
            });
        }
    }

    /**
     * Delete the tag sorted sets.
     *
     * @param array<string> $tagNames Array of tag names
     */
    private function flushTags(array $tagNames): void
    {
        $prefix = $this->context->prefix();

        $this->context->withConnection(function (RedisConnection $conn) use ($prefix, $tagNames) {
            foreach ($tagNames as $name) {
                // Tag key format: "tag:{name}:entries"
                $conn->del($prefix . "tag:{$name}:entries");
            }
        });
    }
}
