<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis;

use DateInterval;
use DateTimeInterface;
use Hypervel\Cache\Contracts\Store;
use Hypervel\Cache\Events\KeyWritten;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\TaggedCache;

class AllTaggedCache extends TaggedCache
{
    /**
     * The cache store implementation.
     *
     * @var RedisStore
     */
    protected Store $store;

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

            return $this->store->allTagOps()->add()->execute(
                $this->itemKey($key),
                $value,
                $seconds,
                $this->tags->tagIds()
            );
        }

        // Null TTL: non-atomic get + forever (matches Repository::add behavior)
        if (is_null($this->get($key))) {
            $result = $this->store->allTagOps()->forever()->execute(
                $this->itemKey($key),
                $value,
                $this->tags->tagIds()
            );

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

        $result = $this->store->allTagOps()->put()->execute(
            $this->itemKey($key),
            $value,
            $seconds,
            $this->tags->tagIds()
        );

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

        $result = $this->store->allTagOps()->putMany()->execute(
            $values,
            $seconds,
            $this->tags->tagIds(),
            sha1($this->tags->getNamespace()) . ':'
        );

        if ($result) {
            foreach ($values as $key => $value) {
                $this->event(new KeyWritten($key, $value, $seconds));
            }
        }

        return $result;
    }

    /**
     * Increment the value of an item in the cache.
     */
    public function increment(string $key, int $value = 1): bool|int
    {
        return $this->store->allTagOps()->increment()->execute(
            $this->itemKey($key),
            $value,
            $this->tags->tagIds()
        );
    }

    /**
     * Decrement the value of an item in the cache.
     */
    public function decrement(string $key, int $value = 1): bool|int
    {
        return $this->store->allTagOps()->decrement()->execute(
            $this->itemKey($key),
            $value,
            $this->tags->tagIds()
        );
    }

    /**
     * Store an item in the cache indefinitely.
     */
    public function forever(string $key, mixed $value): bool
    {
        $result = $this->store->allTagOps()->forever()->execute(
            $this->itemKey($key),
            $value,
            $this->tags->tagIds()
        );

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
        $this->store->allTagOps()->flush()->execute($this->tags->tagIds(), $this->tags->getNames());

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

    /**
     * Get the tag set instance (covariant return type).
     */
    public function getTags(): AllTagSet
    {
        return $this->tags;
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
