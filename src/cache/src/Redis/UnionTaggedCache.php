<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis;

use BadMethodCallException;
use Closure;
use DateInterval;
use DateTimeInterface;
use Generator;
use Hypervel\Cache\Events\CacheHit;
use Hypervel\Cache\Events\CacheMissed;
use Hypervel\Cache\Events\KeyWritten;
use Hypervel\Cache\RedisStore;

/**
 * Union-based tagged cache for Redis 8.0+ enhanced tagging.
 *
 * Key differences from IntersectionTaggedCache:
 * - Tags are for WRITING and FLUSHING only, not for scoped reads
 * - get() throws exception - use Cache::get() directly
 * - flush() deletes items with ANY of the specified tags (union semantics)
 * - Uses HSETEX for automatic hash field expiration
 *
 * Must extend IntersectionTaggedCache to satisfy the return type contract
 * of TaggableStore::tags() which returns TaggedCache.
 */
class UnionTaggedCache extends IntersectionTaggedCache
{
    /**
     * The tag set instance.
     */
    protected UnionTagSet $unionTags;

    /**
     * Create a new tagged cache instance.
     */
    public function __construct(
        RedisStore $store,
        UnionTagSet $tags,
    ) {
        // Call parent with the tag set - parent expects TagSet, we pass UnionTagSet
        // We use a base TagSet to satisfy the parent, but keep our UnionTagSet reference
        parent::__construct($store, $tags);

        // Store reference with narrowed type for our methods
        $this->unionTags = $tags;
    }

    /**
     * Retrieve an item from the cache by key.
     *
     * @throws BadMethodCallException Always - tags are for writing/flushing only
     */
    public function get(array|string $key, mixed $default = null): mixed
    {
        throw new BadMethodCallException(
            'Cannot get items via tags in union mode. Tags are for writing and flushing only. ' .
            'Use Cache::get() directly with the full key.'
        );
    }

    /**
     * Retrieve multiple items from the cache by key.
     *
     * @throws BadMethodCallException Always - tags are for writing/flushing only
     */
    public function many(array $keys): array
    {
        throw new BadMethodCallException(
            'Cannot get items via tags in union mode. Tags are for writing and flushing only. ' .
            'Use Cache::many() directly with the full keys.'
        );
    }

    /**
     * Determine if an item exists in the cache.
     *
     * @throws BadMethodCallException Always - tags are for writing/flushing only
     */
    public function has(array|string $key): bool
    {
        throw new BadMethodCallException(
            'Cannot check existence via tags in union mode. Tags are for writing and flushing only. ' .
            'Use Cache::has() directly with the full key.'
        );
    }

    /**
     * Retrieve an item from the cache and delete it.
     *
     * @throws BadMethodCallException Always - tags are for writing/flushing only
     */
    public function pull(string $key, mixed $default = null): mixed
    {
        throw new BadMethodCallException(
            'Cannot pull items via tags in union mode. Tags are for writing and flushing only. ' .
            'Use Cache::pull() directly with the full key.'
        );
    }

    /**
     * Remove an item from the cache.
     *
     * @throws BadMethodCallException Always - tags are for writing/flushing only
     */
    public function forget(string $key): bool
    {
        throw new BadMethodCallException(
            'Cannot forget items via tags in union mode. Tags are for writing and flushing only. ' .
            'Use Cache::forget() directly with the full key, or flush() to remove all tagged items.'
        );
    }

    /**
     * Store an item in the cache.
     */
    public function put(array|string $key, mixed $value, null|DateInterval|DateTimeInterface|int $ttl = null): bool
    {
        if (is_array($key)) {
            return $this->putMany($key, $value);
        }

        if ($ttl === null) {
            return $this->forever($key, $value);
        }

        $seconds = $this->getSeconds($ttl);

        if ($seconds <= 0) {
            // Can't forget via tags, just return false
            return false;
        }

        $result = $this->store->putWithTags($key, $value, $seconds, $this->unionTags->getNames());

        if ($result) {
            $this->event(new KeyWritten($key, $value, $seconds));
        }

        return $result;
    }

    /**
     * Store multiple items in the cache for a given number of seconds.
     */
    public function putMany(array $values, null|DateInterval|DateTimeInterface|int $ttl = null): bool
    {
        if ($ttl === null) {
            return $this->putManyForever($values);
        }

        $seconds = $this->getSeconds($ttl);

        if ($seconds <= 0) {
            return false;
        }

        $result = $this->store->putManyWithTags($values, $seconds, $this->unionTags->getNames());

        if ($result) {
            foreach ($values as $key => $value) {
                $this->event(new KeyWritten($key, $value, $seconds));
            }
        }

        return $result;
    }

    /**
     * Store an item in the cache if the key does not exist.
     */
    public function add(string $key, mixed $value, null|DateInterval|DateTimeInterface|int $ttl = null): bool
    {
        if ($ttl === null) {
            // Default to 1 year for "null" TTL on add
            $seconds = 31536000;
        } else {
            $seconds = $this->getSeconds($ttl);

            if ($seconds <= 0) {
                return false;
            }
        }

        $result = $this->store->addWithTags($key, $value, $seconds, $this->unionTags->getNames());

        if ($result) {
            $this->event(new KeyWritten($key, $value, $seconds));
        }

        return $result;
    }

    /**
     * Store an item in the cache indefinitely.
     */
    public function forever(string $key, mixed $value): bool
    {
        $result = $this->store->foreverWithTags($key, $value, $this->unionTags->getNames());

        if ($result) {
            $this->event(new KeyWritten($key, $value));
        }

        return $result;
    }

    /**
     * Increment the value of an item in the cache.
     */
    public function increment(string $key, int $value = 1): bool|int
    {
        $result = $this->store->incrementWithTags($key, $value, $this->unionTags->getNames());

        if ($result !== false) {
            $this->event(new KeyWritten($key, $result));
        }

        return $result;
    }

    /**
     * Decrement the value of an item in the cache.
     */
    public function decrement(string $key, int $value = 1): bool|int
    {
        $result = $this->store->decrementWithTags($key, $value, $this->unionTags->getNames());

        if ($result !== false) {
            $this->event(new KeyWritten($key, $result));
        }

        return $result;
    }

    /**
     * Remove all items from the cache that have any of the specified tags.
     */
    public function flush(): bool
    {
        $this->unionTags->flush();

        return true;
    }

    /**
     * Get all items (keys and values) tagged with the current tags.
     *
     * This is useful for debugging or bulk operations on tagged items.
     *
     * @return Generator<string, mixed>
     */
    public function items(): Generator
    {
        return $this->store->tagItems($this->unionTags->getNames());
    }

    /**
     * Get an item from the cache, or execute the given Closure and store the result.
     *
     * @template TCacheValue
     *
     * @param Closure(): TCacheValue $callback
     * @return TCacheValue
     */
    public function remember(string $key, null|DateInterval|DateTimeInterface|int $ttl, Closure $callback): mixed
    {
        // Bypass our own get() which throws an exception
        // Access the store directly to check for existence
        $value = $this->store->get($this->itemKey($key));

        if ($value !== null) {
            $this->event(new CacheHit($key, $value));

            return $value;
        }

        $this->event(new CacheMissed($key));

        $value = $callback();

        $this->put($key, $value, $ttl);

        return $value;
    }

    /**
     * Get an item from the cache, or execute the given Closure and store the result forever.
     *
     * @template TCacheValue
     *
     * @param Closure(): TCacheValue $callback
     * @return TCacheValue
     */
    public function rememberForever(string $key, Closure $callback): mixed
    {
        // Bypass our own get() which throws an exception
        $value = $this->store->get($this->itemKey($key));

        if ($value !== null) {
            $this->event(new CacheHit($key, $value));

            return $value;
        }

        $this->event(new CacheMissed($key));

        $value = $callback();

        $this->forever($key, $value);

        return $value;
    }

    /**
     * Get the tag set instance.
     */
    public function getUnionTags(): UnionTagSet
    {
        return $this->unionTags;
    }

    /**
     * Format the key for a cache item.
     *
     * In union mode, keys are NOT namespaced by tags.
     * Tags are only for invalidation, not for scoping reads.
     */
    protected function itemKey(string $key): string
    {
        return $key;
    }

    /**
     * Store multiple items in the cache indefinitely.
     */
    protected function putManyForever(array $values): bool
    {
        $result = true;

        foreach ($values as $key => $value) {
            if (! $this->forever($key, $value)) {
                $result = false;
            }
        }

        return $result;
    }
}
