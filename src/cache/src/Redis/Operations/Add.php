<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations;

use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Store an item in the cache if the key doesn't exist.
 *
 * Optimized using evalSha to cache the Lua script on the Redis server,
 * avoiding the overhead of re-sending script text on every call.
 *
 * Performance: Uses evalSha with automatic fallback to eval on NOSCRIPT error.
 * This follows the phpredis maintainer's recommendation for script caching.
 */
class Add
{
    /**
     * Lua script for atomic add operation.
     * Checks if key exists, and only sets if it doesn't.
     */
    private const LUA_SCRIPT = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

    /**
     * Create a new add operation instance.
     */
    public function __construct(
        private readonly StoreContext $context,
        private readonly Serialization $serialization,
    ) {}

    /**
     * Execute the add operation.
     *
     * @param string $key The cache key (without prefix)
     * @param mixed $value The value to store (will be serialized)
     * @param int $seconds TTL in seconds (minimum 1)
     * @return bool True if item was added, false if it already exists
     */
    public function execute(string $key, mixed $value, int $seconds): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($key, $value, $seconds) {
            $client = $conn->client();

            // Use serializeForLua since phpredis doesn't auto-serialize ARGV
            $serializedValue = $this->serialization->serializeForLua($value);

            $args = [
                $this->context->prefix() . $key,  // KEYS[1]
                $serializedValue,                  // ARGV[1]
                max(1, $seconds),                  // ARGV[2]
            ];

            $scriptHash = sha1(self::LUA_SCRIPT);

            // Try evalSha first (uses cached script on Redis server)
            $result = $client->evalSha($scriptHash, $args, 1);

            // evalSha returns false on NOSCRIPT error, fallback to eval
            if ($result === false) {
                $result = $client->eval(self::LUA_SCRIPT, $args, 1);
            }

            return (bool) $result;
        });
    }
}
