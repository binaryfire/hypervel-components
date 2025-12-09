<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis;

use Hyperf\Collection\LazyCollection;
use Hypervel\Cache\Contracts\Store;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\TagSet;

class IntersectionTagSet extends TagSet
{
    /**
     * The cache store implementation.
     *
     * @var RedisStore
     */
    protected Store $store;

    /**
     * Add a reference entry to the tag set's underlying sorted set.
     */
    public function addEntry(string $key, int $ttl = 0, ?string $updateWhen = null): void
    {
        $this->store->addIntersectionEntry($key, $ttl, $this->tagIds(), $updateWhen);
    }

    /**
     * Get all of the cache entry keys for the tag set.
     */
    public function entries(): LazyCollection
    {
        return $this->store->getIntersectionEntries($this->tagIds());
    }

    /**
     * Remove the stale entries from the tag set.
     */
    public function flushStaleEntries(): void
    {
        $this->store->flushStaleIntersectionEntries($this->tagIds());
    }

    /**
     * Flush the tag from the cache.
     */
    public function flushTag(string $name): string
    {
        return $this->resetTag($name);
    }

    /**
     * Reset the tag and return the new tag identifier.
     */
    public function resetTag(string $name): string
    {
        $this->store->forget($this->tagKey($name));

        return $this->tagId($name);
    }

    /**
     * Get the unique tag identifier for a given tag.
     */
    public function tagId(string $name): string
    {
        return "tag:{$name}:entries";
    }

    /**
     * Get the tag identifier key for a given tag.
     */
    public function tagKey(string $name): string
    {
        return "tag:{$name}:entries";
    }

    /**
     * Get an array of tag identifiers for all of the tags in the set.
     *
     * @return array<string>
     */
    public function tagIds(): array
    {
        return parent::tagIds();
    }
}
