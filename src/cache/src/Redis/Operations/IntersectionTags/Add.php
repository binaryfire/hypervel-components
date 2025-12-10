<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\IntersectionTags;

use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;
use Redis;

/**
 * Store an item in the cache if it doesn't exist, with intersection tag tracking.
 *
 * Combines the ZADD operations for tag tracking with the atomic add
 * in a single connection checkout for efficiency.
 *
 * Note: Tag entries are always added, even if the key exists. This matches
 * the original behavior where addEntry() is called before checking existence.
 */
class Add
{
    /**
     * Lua script for atomic add (only set if key doesn't exist).
     */
    private const ADD_LUA_SCRIPT = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

    public function __construct(
        private readonly StoreContext $context,
        private readonly Serialization $serialization,
    ) {}

    /**
     * Execute the add operation with tag tracking.
     *
     * @param string $key The cache key (already namespaced by caller)
     * @param mixed $value The value to store
     * @param int $seconds TTL in seconds
     * @param array<string> $tagIds Array of tag identifiers
     * @return bool True if the key was added (didn't exist), false if it already existed
     */
    public function execute(string $key, mixed $value, int $seconds, array $tagIds): bool
    {
        if ($this->context->isCluster()) {
            return $this->executeCluster($key, $value, $seconds, $tagIds);
        }

        return $this->executeStandard($key, $value, $seconds, $tagIds);
    }

    /**
     * Execute for standard Redis.
     *
     * Uses pipeline for ZADD operations, then Lua script for atomic add.
     */
    private function executeStandard(string $key, mixed $value, int $seconds, array $tagIds): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($key, $value, $seconds, $tagIds) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $score = now()->addSeconds($seconds)->getTimestamp();

            // Pipeline the ZADD operations
            if (! empty($tagIds)) {
                $pipeline = $conn->multi(Redis::PIPELINE);

                foreach ($tagIds as $tagId) {
                    $pipeline->zadd($prefix . $tagId, $score, $key);
                }

                $pipeline->exec();
            }

            // Atomic add using Lua script
            $serialized = $this->serialization->serializeForLua($value);
            $args = [
                $prefix . $key,
                $serialized,
                max(1, $seconds),
            ];

            $scriptHash = sha1(self::ADD_LUA_SCRIPT);
            $result = $client->evalSha($scriptHash, $args, 1);

            // Fallback to eval if NOSCRIPT error
            if ($result === false) {
                $result = $client->eval(self::ADD_LUA_SCRIPT, $args, 1);
            }

            return (bool) $result;
        });
    }

    /**
     * Execute for Redis Cluster.
     *
     * Sequential commands since tags and key may be in different slots.
     */
    private function executeCluster(string $key, mixed $value, int $seconds, array $tagIds): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($key, $value, $seconds, $tagIds) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $score = now()->addSeconds($seconds)->getTimestamp();

            // ZADD to each tag's sorted set (sequential - cross-slot)
            foreach ($tagIds as $tagId) {
                $client->zadd($prefix . $tagId, $score, $key);
            }

            // Atomic add using Lua script
            $serialized = $this->serialization->serializeForLua($value);
            $args = [
                $prefix . $key,
                $serialized,
                max(1, $seconds),
            ];

            $scriptHash = sha1(self::ADD_LUA_SCRIPT);
            $result = $client->evalSha($scriptHash, $args, 1);

            // Fallback to eval if NOSCRIPT error
            if ($result === false) {
                $result = $client->eval(self::ADD_LUA_SCRIPT, $args, 1);
            }

            return (bool) $result;
        });
    }
}
