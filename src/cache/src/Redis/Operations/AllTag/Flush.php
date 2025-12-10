<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\AllTag;

use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Flushes all cache entries associated with all tags.
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
     * @param array<string> $tagIds Array of tag identifiers (e.g., "_all:tag:users:entries")
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
     * Uses variadic del() to delete all tag keys in a single Redis call.
     *
     * @param array<string> $tagNames Array of tag names
     */
    private function flushTags(array $tagNames): void
    {
        if (empty($tagNames)) {
            return;
        }

        $this->context->withConnection(function (RedisConnection $conn) use ($tagNames) {
            $tagKeys = array_map(
                fn (string $name) => $this->context->tagHashKey($name),
                $tagNames
            );

            $conn->del(...$tagKeys);
        });
    }
}
