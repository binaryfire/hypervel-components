<?php

declare(strict_types=1);

namespace Hypervel\Cache;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Hypervel\Cache\Contracts\LockProvider;
use Hypervel\Cache\Redis\IntersectionTaggedCache;
use Hypervel\Cache\Redis\IntersectionTagSet;
use Hypervel\Cache\Redis\UnionTaggedCache;
use Hypervel\Cache\Redis\UnionTagSet;
use Hypervel\Cache\Redis\Operations\Add;
use Hypervel\Cache\Redis\Operations\Decrement;
use Hypervel\Cache\Redis\Operations\Flush;
use Hypervel\Cache\Redis\Operations\Forget;
use Hypervel\Cache\Redis\Operations\Forever;
use Hypervel\Cache\Redis\Operations\Get;
use Hypervel\Cache\Redis\Operations\Increment;
use Hypervel\Cache\Redis\Operations\IntersectionTagOperations;
use Hypervel\Cache\Redis\Operations\Many;
use Hypervel\Cache\Redis\Operations\Put;
use Hypervel\Cache\Redis\Operations\PutMany;
use Hypervel\Cache\Redis\Operations\UnionTagOperations;
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
     * The tagging mode ('intersection' or 'union').
     */
    protected string $taggingMode = 'intersection';

    /**
     * Cached StoreContext instance.
     */
    private ?StoreContext $context = null;

    /**
     * Cached Serialization instance.
     */
    private ?Serialization $serialization = null;

    /**
     * Cached shared operation instances.
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
     * Cached tag operation containers.
     */
    private ?UnionTagOperations $unionTagOperations = null;

    private ?IntersectionTagOperations $intersectionTagOperations = null;

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
     * Get the union tag operations container.
     *
     * Use this to access all union-mode tagged cache operations.
     */
    public function unionTagOps(): UnionTagOperations
    {
        return $this->unionTagOperations ??= new UnionTagOperations(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    /**
     * Get the intersection tag operations container.
     *
     * Use this to access all intersection-mode tagged cache operations.
     */
    public function intersectionTagOps(): IntersectionTagOperations
    {
        return $this->intersectionTagOperations ??= new IntersectionTagOperations(
            $this->getContext(),
            $this->getSerialization()
        );
    }

    /**
     * Begin executing a new tags operation.
     */
    public function tags(mixed $names): IntersectionTaggedCache|UnionTaggedCache
    {
        $names = is_array($names) ? $names : func_get_args();

        if ($this->taggingMode === 'union') {
            return new UnionTaggedCache(
                $this,
                new UnionTagSet($this, $names)
            );
        }

        return new IntersectionTaggedCache(
            $this,
            new IntersectionTagSet($this, $names)
        );
    }

    /**
     * Set the tagging mode.
     *
     * @param string $mode Either 'intersection' or 'union'
     */
    public function setTaggingMode(string $mode): static
    {
        $this->taggingMode = in_array($mode, ['intersection', 'union'], true)
            ? $mode
            : 'intersection';

        return $this;
    }

    /**
     * Get the tagging mode.
     */
    public function getTaggingMode(): string
    {
        return $this->taggingMode;
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
        $this->prefix = $prefix;
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
     * Get the Serialization instance.
     */
    public function getSerialization(): Serialization
    {
        return $this->serialization ??= new Serialization($this->getContext());
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

        // Shared operations
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

        // Tag operation containers
        $this->unionTagOperations = null;
        $this->intersectionTagOperations = null;
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
}
