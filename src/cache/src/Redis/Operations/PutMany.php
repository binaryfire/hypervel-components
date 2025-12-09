<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations;

use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Store multiple items in the cache for a given number of seconds.
 *
 * Performance:
 * - Standard mode: Single Lua script execution with evalSha caching
 * - Cluster mode: Uses MULTI/EXEC since RedisCluster doesn't support pipelining
 *   and keys may hash to different slots
 */
class PutMany
{
    /**
     * Lua script for setting multiple keys with the same TTL.
     *
     * KEYS: All cache keys to set
     * ARGV[1]: TTL in seconds
     * ARGV[2..N+1]: Serialized values (matching order of KEYS)
     */
    private const LUA_SCRIPT = "local ttl = ARGV[1] local numKeys = #KEYS for i = 1, numKeys do redis.call('SETEX', KEYS[i], ttl, ARGV[i + 1]) end return true";

    /**
     * Create a new put many operation instance.
     */
    public function __construct(
        private readonly StoreContext $context,
        private readonly Serialization $serialization,
    ) {}

    /**
     * Execute the putMany operation.
     *
     * @param array<string, mixed> $values Key-value pairs to store
     * @param int $seconds TTL in seconds (minimum 1)
     */
    public function execute(array $values, int $seconds): bool
    {
        if (empty($values)) {
            return true;
        }

        // Cluster mode: Keys may hash to different slots, use MULTI
        // (RedisCluster doesn't support pipeline())
        if ($this->context->isCluster()) {
            return $this->executeCluster($values, $seconds);
        }

        // Standard mode: Use Lua script for efficiency
        return $this->executeLua($values, $seconds);
    }

    /**
     * Execute using Lua script for better performance.
     *
     * The Lua script loops through all key-value pairs and executes SETEX
     * for each, reducing Redis command parsing overhead compared to
     * sending N individual SETEX commands.
     */
    private function executeLua(array $values, int $seconds): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($values, $seconds) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $ttl = max(1, $seconds);

            // Build keys and values arrays
            $keys = [];
            $args = [$ttl]; // First arg is TTL

            foreach ($values as $key => $value) {
                $keys[] = $prefix . $key;
                // Use serializeForLua since phpredis doesn't auto-serialize ARGV
                $args[] = $this->serialization->serializeForLua($value);
            }

            // Combine keys and args for eval/evalSha
            // Format: [key1, key2, ..., ttl, val1, val2, ...]
            $evalArgs = array_merge($keys, $args);
            $numKeys = count($keys);

            $scriptHash = sha1(self::LUA_SCRIPT);

            // Try evalSha first (uses cached script on Redis server)
            $result = $client->evalSha($scriptHash, $evalArgs, $numKeys);

            // evalSha returns false on NOSCRIPT error, fallback to eval
            if ($result === false) {
                $result = $client->eval(self::LUA_SCRIPT, $evalArgs, $numKeys);
            }

            return (bool) $result;
        });
    }

    /**
     * Execute for cluster using MULTI/EXEC.
     *
     * In cluster mode, keys may hash to different slots. Unlike standalone Redis,
     * RedisCluster does NOT support pipelining - commands are sent sequentially
     * to each node. MULTI/EXEC still provides value by:
     *
     * 1. Grouping commands into transactions per-node (atomicity per slot)
     * 2. Aggregating results from all nodes into a single array on exec()
     * 3. Matching Laravel's default RedisStore behavior for consistency
     */
    private function executeCluster(array $values, int $seconds): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($values, $seconds) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $ttl = max(1, $seconds);

            // MULTI groups commands by node (does NOT pipeline them)
            $multi = $client->multi();

            foreach ($values as $key => $value) {
                // Use regular serialize() - phpredis auto-serializes for setex
                $multi->setex(
                    $prefix . $key,
                    $ttl,
                    $this->serialization->serialize($value)
                );
            }

            $results = $multi->exec();

            // Check all results succeeded
            if (! is_array($results)) {
                return false;
            }

            foreach ($results as $result) {
                if ($result === false) {
                    return false;
                }
            }

            return true;
        });
    }
}
