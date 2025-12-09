<?php

declare(strict_types=1);

namespace Hypervel\Cache;

use Generator;
use Hyperf\Collection\LazyCollection;
use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Hypervel\Cache\Contracts\LockProvider;
use Hypervel\Cache\Redis\IntersectionTaggedCache;
use Hypervel\Cache\Redis\IntersectionTagSet;
use Hypervel\Cache\Redis\Operations\Add;
use Hypervel\Cache\Redis\Operations\AddWithTags;
use Hypervel\Cache\Redis\Operations\Decrement;
use Hypervel\Cache\Redis\Operations\DecrementWithTags;
use Hypervel\Cache\Redis\Operations\Flush;
use Hypervel\Cache\Redis\Operations\Forget;
use Hypervel\Cache\Redis\Operations\Forever;
use Hypervel\Cache\Redis\Operations\ForeverWithTags;
use Hypervel\Cache\Redis\Operations\Get;
use Hypervel\Cache\Redis\Operations\GetTaggedKeys;
use Hypervel\Cache\Redis\Operations\Increment;
use Hypervel\Cache\Redis\Operations\IncrementWithTags;
use Hypervel\Cache\Redis\Operations\Many;
use Hypervel\Cache\Redis\Operations\Put;
use Hypervel\Cache\Redis\Operations\PutMany;
use Hypervel\Cache\Redis\Operations\PutManyWithTags;
use Hypervel\Cache\Redis\Operations\PutWithTags;
use Hypervel\Cache\Redis\Operations\IntersectionTags\AddEntry as IntersectionAddEntry;
use Hypervel\Cache\Redis\Operations\IntersectionTags\GetEntries as IntersectionGetEntries;
use Hypervel\Cache\Redis\Operations\IntersectionTags\Flush as IntersectionFlush;
use Hypervel\Cache\Redis\Operations\IntersectionTags\FlushStaleEntries as IntersectionFlushStaleEntries;
use Hypervel\Cache\Redis\Operations\TagItems;
use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;

class RedisStore extends TaggableStore implements LockProvider
{
    protected RedisFactory $factory;

    /**
     * The pool factory instance (lazy-loaded if not provided).
     */
    protected ?PoolFactory $poolFactory = null;

    /**
     * A string that should be prepended to keys.
     */
    protected string $prefix;

    /**
     * The Redis connection instance that should be used to manage locks.
     */
    protected string $connection;

    /**
     * The name of the connection that should be used for locks.
     */
    protected string $lockConnection;

    /**
     * Cached StoreContext instance.
     */
    private ?StoreContext $context = null;

    /**
     * Cached Serialization instance.
     */
    private ?Serialization $serialization = null;

    /**
     * Cached operation instances.
     */
    private ?Get $getOperation = null;

    private ?Many $manyOperation = null;

    private ?Put $putOperation = null;

    private ?PutMany $putManyOperation = null;

    private ?Add $addOperation = null;

    private ?Forever $foreverOperation = null;

    private ?Forget $forgetOperation = null;

    private ?Increment $incrementOperation = null;

    private ?Decrement $decrementOperation = null;

    private ?Flush $flushOperation = null;

    /**
     * Cached tagged operation instances.
     */
    private ?PutWithTags $putWithTagsOperation = null;

    private ?AddWithTags $addWithTagsOperation = null;

    private ?ForeverWithTags $foreverWithTagsOperation = null;

    private ?IncrementWithTags $incrementWithTagsOperation = null;

    private ?DecrementWithTags $decrementWithTagsOperation = null;

    private ?PutManyWithTags $putManyWithTagsOperation = null;

    private ?GetTaggedKeys $getTaggedKeysOperation = null;

    private ?TagItems $tagItemsOperation = null;

    /**
     * Cached intersection tag operation instances.
     */
    private ?IntersectionAddEntry $intersectionAddEntryOperation = null;

    private ?IntersectionGetEntries $intersectionGetEntriesOperation = null;

    private ?IntersectionFlushStaleEntries $intersectionFlushStaleEntriesOperation = null;

    private ?IntersectionFlush $intersectionFlushOperation = null;

    /**
     * Create a new Redis store.
     */
    public function __construct(
        RedisFactory $factory,
        string $prefix = '',
        string $connection = 'default',
        ?PoolFactory $poolFactory = null,
    ) {
        $this->factory = $factory;
        $this->poolFactory = $poolFactory;
        $this->setPrefix($prefix);
        $this->setConnection($connection);
    }

    /**
     * Retrieve an item from the cache by key.
     */
    public function get(string $key): mixed
    {
        return $this->getGetOperation()->execute($key);
    }

    /**
     * Retrieve multiple items from the cache by key.
     * Items not found in the cache will have a null value.
     */
    public function many(array $keys): array
    {
        return $this->getManyOperation()->execute($keys);
    }

    /**
     * Store an item in the cache for a given number of seconds.
     */
    public function put(string $key, mixed $value, int $seconds): bool
    {
        return $this->getPutOperation()->execute($key, $value, $seconds);
    }

    /**
     * Store multiple items in the cache for a given number of seconds.
     */
    public function putMany(array $values, int $seconds): bool
    {
        return $this->getPutManyOperation()->execute($values, $seconds);
    }

    /**
     * Store an item in the cache if the key doesn't exist.
     */
    public function add(string $key, mixed $value, int $seconds): bool
    {
        return $this->getAddOperation()->execute($key, $value, $seconds);
    }

    /**
     * Increment the value of an item in the cache.
     */
    public function increment(string $key, int $value = 1): int
    {
        return $this->getIncrementOperation()->execute($key, $value);
    }

    /**
     * Decrement the value of an item in the cache.
     */
    public function decrement(string $key, int $value = 1): int
    {
        return $this->getDecrementOperation()->execute($key, $value);
    }

    /**
     * Store an item in the cache indefinitely.
     */
    public function forever(string $key, mixed $value): bool
    {
        return $this->getForeverOperation()->execute($key, $value);
    }

    /**
     * Get a lock instance.
     */
    public function lock(string $name, int $seconds = 0, ?string $owner = null): RedisLock
    {
        return new RedisLock($this->lockConnection(), $this->prefix . $name, $seconds, $owner);
    }

    /**
     * Restore a lock instance using the owner identifier.
     */
    public function restoreLock(string $name, string $owner): RedisLock
    {
        return $this->lock($name, 0, $owner);
    }

    /**
     * Remove an item from the cache.
     */
    public function forget(string $key): bool
    {
        return $this->getForgetOperation()->execute($key);
    }

    /**
     * Remove all items from the cache.
     */
    public function flush(): bool
    {
        return $this->getFlushOperation()->execute();
    }

    /**
     * Store an item in the cache with tags.
     *
     * @param array<int, string|int> $tags Array of tag names
     */
    public function putWithTags(string $key, mixed $value, int $seconds, array $tags): bool
    {
        return $this->getPutWithTagsOperation()->execute($key, $value, $seconds, $tags);
    }

    /**
     * Store multiple items in the cache with tags.
     *
     * @param array<string, mixed> $values Key-value pairs
     * @param array<int, string|int> $tags Array of tag names
     */
    public function putManyWithTags(array $values, int $seconds, array $tags): bool
    {
        return $this->getPutManyWithTagsOperation()->execute($values, $seconds, $tags);
    }

    /**
     * Store an item in the cache if the key doesn't exist, with tags.
     *
     * @param array<int, string|int> $tags Array of tag names
     */
    public function addWithTags(string $key, mixed $value, int $seconds, array $tags): bool
    {
        return $this->getAddWithTagsOperation()->execute($key, $value, $seconds, $tags);
    }

    /**
     * Store an item in the cache indefinitely with tags.
     *
     * @param array<int, string|int> $tags Array of tag names
     */
    public function foreverWithTags(string $key, mixed $value, array $tags): bool
    {
        return $this->getForeverWithTagsOperation()->execute($key, $value, $tags);
    }

    /**
     * Increment the value of an item in the cache with tags.
     *
     * @param array<int, string|int> $tags Array of tag names
     * @return int|false The new value after incrementing, or false on failure
     */
    public function incrementWithTags(string $key, int $value, array $tags): int|bool
    {
        return $this->getIncrementWithTagsOperation()->execute($key, $value, $tags);
    }

    /**
     * Decrement the value of an item in the cache with tags.
     *
     * @param array<int, string|int> $tags Array of tag names
     * @return int|false The new value after decrementing, or false on failure
     */
    public function decrementWithTags(string $key, int $value, array $tags): int|bool
    {
        return $this->getDecrementWithTagsOperation()->execute($key, $value, $tags);
    }

    /**
     * Get all keys associated with a tag.
     *
     * @return Generator<string> Generator yielding cache keys (without prefix)
     */
    public function getTaggedKeys(string $tag): Generator
    {
        return $this->getGetTaggedKeysOperation()->execute($tag);
    }

    /**
     * Flush all cache items that have any of the specified tags.
     *
     * This is the lazy flush implementation - deletes items, reverse indexes,
     * tag hashes, and updates the registry.
     *
     * @param array<int, string|int> $tags Array of tag names
     */
    public function flushTags(array $tags): void
    {
        // Will be implemented in Phase 7 (FlushTags operation)
        // For now, throw an exception to indicate it's not yet implemented
        throw new \RuntimeException(
            'flushTags() will be implemented in Phase 7. ' .
            'This method requires the FlushTags operation class.'
        );
    }

    /**
     * Get all items (keys and values) for a set of tags.
     *
     * @param array<int, string|int> $tags Array of tag names
     * @return Generator<string, mixed> Generator yielding key => value pairs
     */
    public function tagItems(array $tags): Generator
    {
        return $this->getTagItemsOperation()->execute($tags);
    }

    /**
     * Add a cache key entry to intersection tag sorted sets.
     *
     * @param string $key The cache key (without prefix)
     * @param int $ttl TTL in seconds (0 means forever)
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     * @param string|null $updateWhen Optional ZADD flag: 'NX', 'XX', 'GT', 'LT'
     */
    public function addIntersectionEntry(string $key, int $ttl, array $tagIds, ?string $updateWhen = null): void
    {
        $this->getIntersectionAddEntryOperation()->execute($key, $ttl, $tagIds, $updateWhen);
    }

    /**
     * Get all cache key entries from intersection tag sorted sets.
     *
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     * @return LazyCollection<int, string> Lazy collection yielding cache keys (without prefix)
     */
    public function getIntersectionEntries(array $tagIds): LazyCollection
    {
        return $this->getIntersectionGetEntriesOperation()->execute($tagIds);
    }

    /**
     * Flush stale entries from intersection tag sorted sets.
     *
     * Removes entries with TTL scores that have expired (between 0 and current timestamp).
     * Forever items (score -1) are not affected.
     *
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     */
    public function flushStaleIntersectionEntries(array $tagIds): void
    {
        $this->getIntersectionFlushStaleEntriesOperation()->execute($tagIds);
    }

    /**
     * Flush all cache entries for the given intersection tags.
     *
     * This deletes all cache keys associated with the tags and the tag sorted sets themselves.
     *
     * @param array<string> $tagIds Array of tag identifiers (e.g., "tag:users:entries")
     * @param array<string> $tagNames Array of tag names (e.g., ["users", "posts"])
     */
    public function flushIntersectionTags(array $tagIds, array $tagNames): void
    {
        $this->getIntersectionFlushOperation()->execute($tagIds, $tagNames);
    }

    /**
     * Begin executing a new tags operation.
     */
    public function tags(mixed $names): IntersectionTaggedCache
    {
        return new IntersectionTaggedCache(
            $this,
            new IntersectionTagSet($this, is_array($names) ? $names : func_get_args())
        );
    }

    /**
     * Get the Redis connection instance.
     */
    public function connection(): RedisProxy
    {
        return $this->factory->get($this->connection);
    }

    /**
     * Get the Redis connection instance that should be used to manage locks.
     */
    public function lockConnection(): RedisProxy
    {
        return $this->factory->get($this->lockConnection ?? $this->connection);
    }

    /**
     * Specify the name of the connection that should be used to store data.
     */
    public function setConnection(string $connection): void
    {
        $this->connection = $connection;
        $this->clearCachedInstances();
    }

    /**
     * Specify the name of the connection that should be used to manage locks.
     */
    public function setLockConnection(string $connection): static
    {
        $this->lockConnection = $connection;

        return $this;
    }

    /**
     * Get the cache key prefix.
     */
    public function getPrefix(): string
    {
        return $this->prefix;
    }

    /**
     * Get the Redis database instance.
     */
    public function getRedis(): RedisFactory
    {
        return $this->factory;
    }

    /**
     * Set the cache key prefix.
     */
    public function setPrefix(string $prefix): void
    {
        $this->prefix = ! empty($prefix) ? $prefix . ':' : '';
        $this->clearCachedInstances();
    }

    /**
     * Get the StoreContext instance.
     */
    public function getContext(): StoreContext
    {
        return $this->context ??= new StoreContext(
            $this->getPoolFactory(),
            $this->connection,
            $this->prefix
        );
    }

    /**
     * Get the PoolFactory instance, lazily resolving if not provided.
     */
    protected function getPoolFactory(): PoolFactory
    {
        return $this->poolFactory ??= $this->resolvePoolFactory();
    }

    /**
     * Serialize the value.
     */
    protected function serialize(mixed $value): mixed
    {
        return $this->getSerialization()->serialize($value);
    }

    /**
     * Unserialize the value.
     */
    protected function unserialize(mixed $value): mixed
    {
        return $this->getSerialization()->unserialize($value);
    }

    /**
     * Get the Serialization instance.
     */
    private function getSerialization(): Serialization
    {
        return $this->serialization ??= new Serialization($this->getContext());
    }

    /**
     * Resolve the PoolFactory from the container.
     */
    private function resolvePoolFactory(): PoolFactory
    {
        return \Hyperf\Support\make(PoolFactory::class);
    }

    /**
     * Clear all cached instances when connection or prefix changes.
     */
    private function clearCachedInstances(): void
    {
        $this->context = null;
        $this->serialization = null;
        $this->getOperation = null;
        $this->manyOperation = null;
        $this->putOperation = null;
        $this->putManyOperation = null;
        $this->addOperation = null;
        $this->foreverOperation = null;
        $this->forgetOperation = null;
        $this->incrementOperation = null;
        $this->decrementOperation = null;
        $this->flushOperation = null;
        // Tagged operations (union mode)
        $this->putWithTagsOperation = null;
        $this->addWithTagsOperation = null;
        $this->foreverWithTagsOperation = null;
        $this->incrementWithTagsOperation = null;
        $this->decrementWithTagsOperation = null;
        $this->putManyWithTagsOperation = null;
        $this->getTaggedKeysOperation = null;
        $this->tagItemsOperation = null;
        // Tagged operations (intersection mode)
        $this->intersectionAddEntryOperation = null;
        $this->intersectionGetEntriesOperation = null;
        $this->intersectionFlushStaleEntriesOperation = null;
        $this->intersectionFlushOperation = null;
    }

    private function getGetOperation(): Get
    {
        return $this->getOperation ??= new Get(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getManyOperation(): Many
    {
        return $this->manyOperation ??= new Many(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getPutOperation(): Put
    {
        return $this->putOperation ??= new Put(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getPutManyOperation(): PutMany
    {
        return $this->putManyOperation ??= new PutMany(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getAddOperation(): Add
    {
        return $this->addOperation ??= new Add(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getForeverOperation(): Forever
    {
        return $this->foreverOperation ??= new Forever(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getForgetOperation(): Forget
    {
        return $this->forgetOperation ??= new Forget($this->getContext());
    }

    private function getIncrementOperation(): Increment
    {
        return $this->incrementOperation ??= new Increment($this->getContext());
    }

    private function getDecrementOperation(): Decrement
    {
        return $this->decrementOperation ??= new Decrement($this->getContext());
    }

    private function getFlushOperation(): Flush
    {
        return $this->flushOperation ??= new Flush($this->getContext());
    }

    private function getPutWithTagsOperation(): PutWithTags
    {
        return $this->putWithTagsOperation ??= new PutWithTags(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getAddWithTagsOperation(): AddWithTags
    {
        return $this->addWithTagsOperation ??= new AddWithTags(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getForeverWithTagsOperation(): ForeverWithTags
    {
        return $this->foreverWithTagsOperation ??= new ForeverWithTags(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getIncrementWithTagsOperation(): IncrementWithTags
    {
        return $this->incrementWithTagsOperation ??= new IncrementWithTags(
            $this->getContext()
        );
    }

    private function getDecrementWithTagsOperation(): DecrementWithTags
    {
        return $this->decrementWithTagsOperation ??= new DecrementWithTags(
            $this->getContext()
        );
    }

    private function getPutManyWithTagsOperation(): PutManyWithTags
    {
        return $this->putManyWithTagsOperation ??= new PutManyWithTags(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    private function getGetTaggedKeysOperation(): GetTaggedKeys
    {
        return $this->getTaggedKeysOperation ??= new GetTaggedKeys(
            $this->getContext()
        );
    }

    private function getTagItemsOperation(): TagItems
    {
        return $this->tagItemsOperation ??= new TagItems(
            $this->getContext(),
            $this->getSerialization(),
            $this->getGetTaggedKeysOperation()
        );
    }

    private function getIntersectionAddEntryOperation(): IntersectionAddEntry
    {
        return $this->intersectionAddEntryOperation ??= new IntersectionAddEntry(
            $this->getContext()
        );
    }

    private function getIntersectionGetEntriesOperation(): IntersectionGetEntries
    {
        return $this->intersectionGetEntriesOperation ??= new IntersectionGetEntries(
            $this->getContext()
        );
    }

    private function getIntersectionFlushStaleEntriesOperation(): IntersectionFlushStaleEntries
    {
        return $this->intersectionFlushStaleEntriesOperation ??= new IntersectionFlushStaleEntries(
            $this->getContext()
        );
    }

    private function getIntersectionFlushOperation(): IntersectionFlush
    {
        return $this->intersectionFlushOperation ??= new IntersectionFlush(
            $this->getContext(),
            $this->getIntersectionGetEntriesOperation()
        );
    }
}
