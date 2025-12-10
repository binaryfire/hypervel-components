<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis;

use Generator;
use Hypervel\Cache\Contracts\Store;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\TagSet;

/**
 * Union-based tag set for Redis 8.0+ enhanced tagging.
 *
 * Key differences from IntersectionTagSet:
 * - Tag IDs are just the tag names (no random UUID versioning)
 * - Uses hashes with HSETEX for automatic field expiration
 * - Tags track which keys belong to them (for union flush semantics)
 * - Flush affects items with ANY of the specified tags (union), not ALL (intersection)
 *
 * This class is intentionally simple - it delegates most work to RedisStore
 * and the tagged operation classes in Redis/Operations/.
 */
class UnionTagSet extends TagSet
{
    /**
     * The cache store implementation.
     *
     * @var RedisStore
     */
    protected Store $store;

    /**
     * Create a new UnionTagSet instance.
     */
    public function __construct(RedisStore $store, array $names = [])
    {
        parent::__construct($store, $names);
    }

    /**
     * Get the unique tag identifier for a given tag.
     *
     * Unlike IntersectionTagSet which uses random UUIDs, union mode
     * uses the tag name directly. This means tags don't get "versioned"
     * on flush - actual cache keys are deleted instead.
     */
    public function tagId(string $name): string
    {
        return $name;
    }

    /**
     * Get an array of tag identifiers for all of the tags in the set.
     */
    public function tagIds(): array
    {
        return $this->names;
    }

    /**
     * Get the hash key for a tag.
     *
     * Format: {prefix}_erc:tag:{tag}:entries
     */
    public function tagHashKey(string $name): string
    {
        return $this->getRedisStore()->getContext()->tagHashKey($name);
    }

    /**
     * Get all cache keys for this tag set (union of all tags).
     *
     * This is a generator that yields unique keys across all tags.
     * Used for listing tagged items or bulk operations.
     */
    public function entries(): Generator
    {
        $seen = [];

        foreach ($this->names as $name) {
            foreach ($this->getRedisStore()->unionTagOps()->getTaggedKeys()->execute($name) as $key) {
                if (! isset($seen[$key])) {
                    $seen[$key] = true;
                    yield $key;
                }
            }
        }
    }

    /**
     * Reset the tag set.
     *
     * In union mode, this actually deletes the cached items,
     * unlike intersection mode which just changes the tag version.
     */
    public function reset(): void
    {
        $this->flush();
    }

    /**
     * Flush all tags in this set.
     *
     * Deletes all cache items that have ANY of the specified tags
     * (union semantics), along with their reverse indexes and tag hashes.
     */
    public function flush(): void
    {
        $this->getRedisStore()->unionTagOps()->flush()->execute($this->names);
    }

    /**
     * Flush a single tag.
     */
    public function flushTag(string $name): string
    {
        $this->getRedisStore()->unionTagOps()->flush()->execute([$name]);

        return $this->tagKey($name);
    }

    /**
     * Get a unique namespace that changes when any of the tags are flushed.
     *
     * Not used in union mode since we don't namespace keys by tags.
     * Returns empty string for compatibility with TaggedCache.
     */
    public function getNamespace(): string
    {
        return '';
    }

    /**
     * Reset the tag and return the new tag identifier.
     *
     * In union mode, this flushes the tag and returns the tag name.
     * The tag name never changes (unlike intersection mode's UUIDs).
     */
    public function resetTag(string $name): string
    {
        $this->flushTag($name);

        return $name;
    }

    /**
     * Get the tag key for a given tag name.
     *
     * Returns the hash key for the tag (same as tagHashKey).
     */
    public function tagKey(string $name): string
    {
        return $this->tagHashKey($name);
    }

    /**
     * Get the store as a RedisStore instance.
     */
    protected function getRedisStore(): RedisStore
    {
        /** @var RedisStore $store */
        $store = $this->store;

        return $store;
    }
}
