<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis;

use Hyperf\Collection\LazyCollection;
use Hypervel\Cache\Contracts\Store;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\TagSet;
use Hypervel\Redis\RedisConnection;
use Redis;

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
        $ttl = $ttl > 0 ? now()->addSeconds($ttl)->getTimestamp() : -1;

        $this->store->getContext()->withConnection(function (RedisConnection $conn) use ($key, $ttl, $updateWhen) {
            foreach ($this->tagIds() as $tagKey) {
                $prefixedKey = $this->store->getPrefix() . $tagKey;
                if ($updateWhen) {
                    $conn->zadd($prefixedKey, $updateWhen, $ttl, $key);
                } else {
                    $conn->zadd($prefixedKey, $ttl, $key);
                }
            }
        });
    }

    /**
     * Get all of the cache entry keys for the tag set.
     */
    public function entries(): LazyCollection
    {
        $context = $this->store->getContext();
        $prefix = $this->store->getPrefix();

        $defaultCursorValue = match (true) {
            version_compare(phpversion('redis'), '6.1.0', '>=') => null,
            default => '0',
        };

        return new LazyCollection(function () use ($context, $prefix, $defaultCursorValue) {
            foreach ($this->tagIds() as $tagKey) {
                // Collect all entries for this tag within one connection hold
                $tagEntries = $context->withConnection(function (RedisConnection $conn) use ($prefix, $tagKey, $defaultCursorValue) {
                    $cursor = $defaultCursorValue;
                    $allEntries = [];

                    do {
                        $entries = $conn->zScan(
                            $prefix . $tagKey,
                            $cursor,
                            '*',
                            1000
                        );

                        if (! is_array($entries)) {
                            break;
                        }

                        $allEntries = array_merge($allEntries, array_keys($entries));
                    } while (((string) $cursor) !== $defaultCursorValue);

                    return array_unique($allEntries);
                });

                foreach ($tagEntries as $entry) {
                    yield $entry;
                }
            }
        });
    }

    /**
     * Remove the stale entries from the tag set.
     */
    public function flushStaleEntries(): void
    {
        $this->store->getContext()->withConnection(function (RedisConnection $conn) {
            $pipeline = $conn->multi(Redis::PIPELINE);

            foreach ($this->tagIds() as $tagKey) {
                $pipeline->zRemRangeByScore(
                    $this->store->getPrefix() . $tagKey,
                    '0',
                    (string) now()->getTimestamp()
                );
            }

            $pipeline->exec();
        });
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
}
