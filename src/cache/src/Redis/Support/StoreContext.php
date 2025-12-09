<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Support;

use Hyperf\Redis\Pool\PoolFactory;
use Hypervel\Redis\RedisConnection;
use Redis;
use RedisCluster;

/**
 * Shared context for Redis cache operations.
 *
 * This class encapsulates the dependencies that all cache operations need,
 * providing a clean interface to Redis connection, client, and configuration.
 */
class StoreContext
{
    /**
     * The segment used for tag hash keys.
     * Full tag hash key format: {prefix}_erc:tag:{tag}:entries
     */
    public const TAG_SEGMENT = '_erc:tag:';

    /**
     * The suffix for tag hash keys (appended after tag name).
     */
    public const TAG_HASH_SUFFIX = ':entries';

    /**
     * The suffix used for reverse index keys.
     * Full reverse index key format: {prefix}{key}:_erc:tags
     */
    public const REVERSE_INDEX_SUFFIX = ':_erc:tags';

    /**
     * The name of the tag registry sorted set.
     * Full registry key format: {prefix}_erc:tag:registry
     */
    public const TAG_REGISTRY_NAME = 'registry';

    /**
     * The maximum expiry timestamp (Year 9999) for "forever" items.
     * Used in the tag registry to represent items with no expiration.
     */
    public const MAX_EXPIRY = 253402300799;

    /**
     * The value stored in tag hash fields.
     * We only need to track membership, so we use a minimal placeholder value.
     */
    public const TAG_FIELD_VALUE = '1';

    public function __construct(
        private readonly PoolFactory $poolFactory,
        private readonly string $connectionName,
        private readonly string $prefix,
    ) {}

    /**
     * Get the cache key prefix.
     */
    public function prefix(): string
    {
        return $this->prefix;
    }

    /**
     * Get the connection name.
     */
    public function connectionName(): string
    {
        return $this->connectionName;
    }

    /**
     * Get the tag prefix (includes cache prefix).
     */
    public function tagPrefix(): string
    {
        return $this->prefix . self::TAG_SEGMENT;
    }

    /**
     * Get the full tag hash key for a given tag.
     */
    public function tagHashKey(string $tag): string
    {
        return $this->tagPrefix() . $tag . self::TAG_HASH_SUFFIX;
    }

    /**
     * Get the tag hash suffix (for Lua scripts that build keys dynamically).
     */
    public function tagHashSuffix(): string
    {
        return self::TAG_HASH_SUFFIX;
    }

    /**
     * Get the full reverse index key for a cache key.
     */
    public function reverseIndexKey(string $key): string
    {
        return $this->prefix . $key . self::REVERSE_INDEX_SUFFIX;
    }

    /**
     * Get the tag registry key (without OPT_PREFIX).
     */
    public function registryKey(): string
    {
        return $this->tagPrefix() . self::TAG_REGISTRY_NAME;
    }

    /**
     * Execute callback with a held connection from the pool.
     *
     * Use this for operations requiring multiple commands on the same
     * connection (cluster mode, complex transactions). The connection
     * is automatically returned to the pool after the callback completes.
     *
     * @template T
     * @param callable(RedisConnection): T $callback
     * @return T
     */
    public function withConnection(callable $callback): mixed
    {
        $pool = $this->poolFactory->getPool($this->connectionName);
        /** @var RedisConnection $connection */
        $connection = $pool->get();

        try {
            return $callback($connection);
        } finally {
            $connection->release();
        }
    }

    /**
     * Check if the connection is a Redis Cluster.
     */
    public function isCluster(): bool
    {
        return $this->withConnection(
            fn (RedisConnection $conn) => $conn->client() instanceof RedisCluster
        );
    }

    /**
     * Get the OPT_PREFIX value from the Redis client.
     */
    public function optPrefix(): string
    {
        return $this->withConnection(
            fn (RedisConnection $conn) => (string) $conn->client()->getOption(Redis::OPT_PREFIX)
        );
    }

    /**
     * Get the full tag prefix including OPT_PREFIX (for Lua scripts).
     */
    public function fullTagPrefix(): string
    {
        return $this->optPrefix() . $this->tagPrefix();
    }

    /**
     * Get the full reverse index key including OPT_PREFIX (for Lua scripts).
     */
    public function fullReverseIndexKey(string $key): string
    {
        return $this->optPrefix() . $this->prefix . $key . self::REVERSE_INDEX_SUFFIX;
    }

    /**
     * Get the full registry key including OPT_PREFIX (for Lua scripts).
     */
    public function fullRegistryKey(): string
    {
        return $this->optPrefix() . $this->tagPrefix() . self::TAG_REGISTRY_NAME;
    }
}
