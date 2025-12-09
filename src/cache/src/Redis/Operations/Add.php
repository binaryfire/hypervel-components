<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations;

use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Store an item in the cache if it doesn't exist (non-tagged).
 *
 * Optimizes Laravel's default add() by using evalSha to cache the Lua script,
 * avoiding the overhead of re-sending script text on every call.
 *
 * Performance: Uses evalSha with automatic fallback to eval on NOSCRIPT.
 */
class Add
{
    /**
     * The Lua script for atomic add operation.
     * Must match Laravel's LuaScripts::add() exactly for hash compatibility.
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
     * @param int $seconds TTL in seconds (must be > 0)
     * @return bool True if item was added, false if it already exists or on failure
     */
    public function execute(string $key, mixed $value, int $seconds): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($key, $value, $seconds) {
            $client = $conn->client();
            $prefix = $this->context->prefix();

            // Use serialization helper for Lua arguments
            $serializedValue = $this->serialization->serializeForLua($value);

            $args = [
                $prefix . $key,       // KEYS[1]
                $serializedValue,     // ARGV[1]
                max(1, $seconds),     // ARGV[2]
            ];

            $scriptHash = sha1(self::LUA_SCRIPT);
            $result = $client->evalSha($scriptHash, $args, 1);

            // evalSha returns false if script not loaded (NOSCRIPT), fall back to eval
            if ($result === false) {
                $result = $client->eval(self::LUA_SCRIPT, $args, 1);
            }

            return (bool) $result;
        });
    }
}
