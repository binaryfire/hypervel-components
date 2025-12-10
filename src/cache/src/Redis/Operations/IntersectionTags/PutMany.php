<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\IntersectionTags;

use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;
use Redis;

/**
 * Store multiple items in the cache with intersection tag tracking.
 *
 * Combines the ZADD operations for all keys to all tags with SETEX
 * for each cache value in a single pipeline for efficiency.
 */
class PutMany
{
    public function __construct(
        private readonly StoreContext $context,
        private readonly Serialization $serialization,
    ) {}

    /**
     * Execute the putMany operation with tag tracking.
     *
     * @param array<string, mixed> $values Key-value pairs (keys already namespaced)
     * @param int $seconds TTL in seconds
     * @param array<string> $tagIds Array of tag identifiers
     * @param string $namespace The namespace prefix for keys (for building namespaced keys)
     * @return bool True if all operations successful
     */
    public function execute(array $values, int $seconds, array $tagIds, string $namespace): bool
    {
        if (empty($values)) {
            return true;
        }

        if ($this->context->isCluster()) {
            return $this->executeCluster($values, $seconds, $tagIds, $namespace);
        }

        return $this->executePipeline($values, $seconds, $tagIds, $namespace);
    }

    /**
     * Execute using pipeline for standard Redis (non-cluster).
     */
    private function executePipeline(array $values, int $seconds, array $tagIds, string $namespace): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($values, $seconds, $tagIds, $namespace) {
            $prefix = $this->context->prefix();
            $score = now()->addSeconds($seconds)->getTimestamp();
            $ttl = max(1, $seconds);

            $pipeline = $conn->multi(Redis::PIPELINE);

            foreach ($values as $key => $value) {
                $namespacedKey = $namespace . $key;
                $serialized = $this->serialization->serialize($value);

                // ZADD to each tag's sorted set for this key
                foreach ($tagIds as $tagId) {
                    $pipeline->zadd($prefix . $tagId, $score, $namespacedKey);
                }

                // SETEX for the cache value
                $pipeline->setex($prefix . $namespacedKey, $ttl, $serialized);
            }

            $results = $pipeline->exec();

            return $results !== false && ! in_array(false, $results, true);
        });
    }

    /**
     * Execute using sequential commands for Redis Cluster.
     */
    private function executeCluster(array $values, int $seconds, array $tagIds, string $namespace): bool
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($values, $seconds, $tagIds, $namespace) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $score = now()->addSeconds($seconds)->getTimestamp();
            $ttl = max(1, $seconds);

            $allSucceeded = true;

            foreach ($values as $key => $value) {
                $namespacedKey = $namespace . $key;
                $serialized = $this->serialization->serialize($value);

                // ZADD to each tag's sorted set for this key
                foreach ($tagIds as $tagId) {
                    $client->zadd($prefix . $tagId, $score, $namespacedKey);
                }

                // SETEX for the cache value
                if (! $client->setex($prefix . $namespacedKey, $ttl, $serialized)) {
                    $allSucceeded = false;
                }
            }

            return $allSucceeded;
        });
    }
}
