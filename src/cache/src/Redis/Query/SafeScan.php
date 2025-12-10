<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Query;

use Generator;
use Redis;
use RedisCluster;

/**
 * Safely scan the Redis keyspace for keys matching a pattern.
 *
 * This class provides a memory-efficient iterator over keys using the SCAN command,
 * correctly handling the complexity of Redis OPT_PREFIX configuration.
 *
 * ## The OPT_PREFIX Problem
 *
 * phpredis has an OPT_PREFIX option that automatically prepends a prefix to keys
 * for most commands (GET, SET, DEL, etc.). However, this creates complexity:
 *
 * 1. **SCAN does NOT auto-prefix the pattern** - You must manually include OPT_PREFIX
 *    in your SCAN pattern to match keys that were stored with auto-prefixing.
 *
 * 2. **SCAN returns full keys** - Keys returned include the OPT_PREFIX as stored in Redis.
 *
 * 3. **DEL DOES auto-prefix** - If you pass a SCAN result directly to DEL, phpredis
 *    adds OPT_PREFIX again, causing double-prefixing and failed deletions.
 *
 * ## Example of the Bug This Class Prevents
 *
 * ```
 * OPT_PREFIX = "myapp:"
 * Stored key in Redis = "myapp:cache:user:1"
 *
 * // WRONG approach (what broken code does):
 * $keys = $redis->scan($iter, "myapp:cache:*");  // Returns ["myapp:cache:user:1"]
 * $redis->del($keys[0]);  // Tries to delete "myapp:myapp:cache:user:1" - FAILS!
 *
 * // CORRECT approach (what SafeScan does):
 * $keys = $redis->scan($iter, "myapp:cache:*");  // Returns ["myapp:cache:user:1"]
 * $strippedKey = substr($keys[0], strlen("myapp:"));  // "cache:user:1"
 * $redis->del($strippedKey);  // phpredis adds prefix -> deletes "myapp:cache:user:1" - SUCCESS!
 * ```
 *
 * ## Usage
 *
 * This class is designed to be used within a connection pool callback:
 *
 * ```php
 * $context->withConnection(function (RedisConnection $conn) {
 *     $safeScan = new SafeScan($conn->client(), $optPrefix);
 *     foreach ($safeScan->execute('cache:users:*') as $key) {
 *         // $key is stripped of OPT_PREFIX, safe to use with del(), get(), etc.
 *     }
 * });
 * ```
 */
final class SafeScan
{
    /**
     * Create a new safe scan instance.
     *
     * @param Redis|RedisCluster $client The raw Redis client (from $connection->client())
     * @param string $optPrefix The OPT_PREFIX value (from $client->getOption(Redis::OPT_PREFIX))
     */
    public function __construct(
        private readonly Redis|RedisCluster $client,
        private readonly string $optPrefix,
    ) {}

    /**
     * Execute the scan operation.
     *
     * @param string $pattern The pattern to match (e.g., "cache:users:*").
     *                        Should NOT include OPT_PREFIX - it will be added automatically.
     * @param int $count The COUNT hint for SCAN (not a limit, just a hint to Redis)
     * @return Generator<string> Yields keys with OPT_PREFIX stripped, safe for use with
     *                           other phpredis commands that auto-add the prefix.
     */
    public function execute(string $pattern, int $count = 1000): Generator
    {
        $prefixLen = strlen($this->optPrefix);

        // SCAN does not automatically apply OPT_PREFIX to the pattern,
        // so we must prepend it manually to match keys stored with auto-prefixing.
        $scanPattern = $pattern;
        if ($prefixLen > 0 && ! str_starts_with($pattern, $this->optPrefix)) {
            $scanPattern = $this->optPrefix . $pattern;
        }

        // phpredis 6.1.0+ uses null as initial cursor, older versions use 0
        $iterator = match (true) {
            version_compare(phpversion('redis') ?: '0', '6.1.0', '>=') => null,
            default => 0,
        };

        do {
            // SCAN returns keys as they exist in Redis (with full prefix)
            $keys = $this->client->scan($iterator, $scanPattern, $count);

            // Normalize result (phpredis returns false on failure/empty)
            if ($keys === false || ! is_array($keys)) {
                $keys = [];
            }

            // Yield keys with OPT_PREFIX stripped so they can be used directly
            // with other phpredis commands that auto-add the prefix.
            foreach ($keys as $key) {
                if ($prefixLen > 0 && str_starts_with($key, $this->optPrefix)) {
                    yield substr($key, $prefixLen);
                } else {
                    yield $key;
                }
            }
        } while ($iterator > 0);
    }
}
