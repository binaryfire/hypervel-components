<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis;

use DateInterval;
use DateTimeInterface;
use Hypervel\Cache\Contracts\Store;
use Hypervel\Cache\Events\KeyWritten;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\TaggedCache;
use Hypervel\Cache\TagSet;

class IntersectionTaggedCache extends TaggedCache
{
    /**
     * The cache store implementation.
     *
     * @var RedisStore
     */
    protected Store $store;

    /**
     * The tag set instance.
     *
     * @var IntersectionTagSet
     */
    protected TagSet $tags;

    /**
     * Store an item in the cache if the key does not exist.
     */
    public function add(string $key, mixed $value, null|DateInterval|DateTimeInterface|int $ttl = null): bool
    {
        if ($ttl !== null) {
            $seconds = $this->getSeconds($ttl);

            if ($seconds <= 0) {
                return false;
            }

            $this->tags->addEntry($this->itemKey($key), $seconds);

            // RedisStore has atomic add() method
            return $this->store->add($this->itemKey($key), $value, $seconds);
        }

        // Null TTL: non-atomic get + forever (matches Repository::add behavior)
        $this->tags->addEntry($this->itemKey($key), 0);

        if (is_null($this->get($key))) {
            // Call store->forever directly to avoid double tag entry via $this->forever()
            $result = $this->store->forever($this->itemKey($key), $value);

            if ($result) {
                $this->event(new KeyWritten($key, $value));
            }

            return $result;
        }

        return false;
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
            return $this->forget($key);
        }

        $this->tags->addEntry($this->itemKey($key), $seconds);

        $result = $this->store->put($this->itemKey($key), $value, $seconds);

        if ($result) {
            $this->event(new KeyWritten($key, $value, $seconds));
        }

        return $result;
    }

    /**
     * Increment the value of an item in the cache.
     */
    public function increment(string $key, int $value = 1): bool|int
    {
        $this->tags->addEntry($this->itemKey($key), updateWhen: 'NX');

        return $this->store->increment($this->itemKey($key), $value);
    }

    /**
     * Decrement the value of an item in the cache.
     */
    public function decrement(string $key, int $value = 1): bool|int
    {
        $this->tags->addEntry($this->itemKey($key), updateWhen: 'NX');

        return $this->store->decrement($this->itemKey($key), $value);
    }

    /**
     * Store an item in the cache indefinitely.
     */
    public function forever(string $key, mixed $value): bool
    {
        $this->tags->addEntry($this->itemKey($key));

        $result = $this->store->forever($this->itemKey($key), $value);

        if ($result) {
            $this->event(new KeyWritten($key, $value));
        }

        return $result;
    }

    /**
     * Remove all items from the cache.
     */
    public function flush(): bool
    {
        $this->store->flushIntersectionTags($this->tags->tagIds(), $this->tags->getNames());

        return true;
    }

    /**
     * Remove all stale reference entries from the tag set.
     */
    public function flushStale(): bool
    {
        $this->tags->flushStaleEntries();

        return true;
    }
}
