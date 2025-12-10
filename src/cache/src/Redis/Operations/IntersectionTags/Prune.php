<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\IntersectionTags;

use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;
use Redis;
use RedisCluster;

/**
 * Prune stale entries from intersection tag sorted sets.
 *
 * This operation performs a complete cleanup of intersection-mode tag data:
 * 1. Discovers all tag sorted sets via SCAN (pattern: {prefix}tag:*:entries)
 * 2. Removes stale entries via ZREMRANGEBYSCORE (scores between 0 and now)
 * 3. Deletes empty sorted sets (ZCARD == 0)
 *
 * Forever items (score = -1) are preserved since ZREMRANGEBYSCORE uses 0 as
 * the lower bound, excluding negative scores.
 *
 * @see https://redis.io/commands/scan/
 * @see https://redis.io/commands/zremrangebyscore/
 */
class Prune
{
    /**
     * Default number of keys to process per SCAN iteration.
     */
    private const DEFAULT_SCAN_COUNT = 1000;

    /**
     * Number of tags to process in each batch for ZREMRANGEBYSCORE/ZCARD/DEL.
     */
    private const BATCH_SIZE = 100;

    /**
     * Create a new prune operation instance.
     */
    public function __construct(
        private readonly StoreContext $context,
    ) {}

    /**
     * Execute the prune operation.
     *
     * @param int $scanCount Number of keys per SCAN iteration
     * @return array{tags_scanned: int, entries_removed: int, empty_sets_deleted: int}
     */
    public function execute(int $scanCount = self::DEFAULT_SCAN_COUNT): array
    {
        if ($this->context->isCluster()) {
            return $this->executeCluster($scanCount);
        }

        return $this->executePipeline($scanCount);
    }

    /**
     * Execute using pipeline for standard Redis.
     *
     * @return array{tags_scanned: int, entries_removed: int, empty_sets_deleted: int}
     */
    private function executePipeline(int $scanCount): array
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($scanCount) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $pattern = $prefix . 'tag:*:entries';
            $now = time();

            $tagsScanned = 0;
            $entriesRemoved = 0;
            $emptySetsDeleted = 0;

            // Discover and process tags in batches
            $tagBatch = [];

            foreach ($this->scanForTags($client, $pattern, $scanCount) as $tagKey) {
                $tagBatch[] = $tagKey;

                if (count($tagBatch) >= self::BATCH_SIZE) {
                    $result = $this->processBatchPipeline($client, $tagBatch, $now);
                    $tagsScanned += $result['scanned'];
                    $entriesRemoved += $result['removed'];
                    $emptySetsDeleted += $result['deleted'];
                    $tagBatch = [];
                }
            }

            // Process remaining tags
            if (! empty($tagBatch)) {
                $result = $this->processBatchPipeline($client, $tagBatch, $now);
                $tagsScanned += $result['scanned'];
                $entriesRemoved += $result['removed'];
                $emptySetsDeleted += $result['deleted'];
            }

            return [
                'tags_scanned' => $tagsScanned,
                'entries_removed' => $entriesRemoved,
                'empty_sets_deleted' => $emptySetsDeleted,
            ];
        });
    }

    /**
     * Execute using sequential commands for Redis Cluster.
     *
     * @return array{tags_scanned: int, entries_removed: int, empty_sets_deleted: int}
     */
    private function executeCluster(int $scanCount): array
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($scanCount) {
            $client = $conn->client();
            $prefix = $this->context->prefix();
            $pattern = $prefix . 'tag:*:entries';
            $now = time();

            $tagsScanned = 0;
            $entriesRemoved = 0;
            $emptySetsDeleted = 0;

            foreach ($this->scanForTags($client, $pattern, $scanCount) as $tagKey) {
                $tagsScanned++;

                // Remove stale entries
                $removed = $client->zRemRangeByScore($tagKey, '0', (string) $now);
                $entriesRemoved += is_int($removed) ? $removed : 0;

                // Delete if empty
                $count = $client->zCard($tagKey);
                if ($count === 0) {
                    $client->del($tagKey);
                    $emptySetsDeleted++;
                }
            }

            return [
                'tags_scanned' => $tagsScanned,
                'entries_removed' => $entriesRemoved,
                'empty_sets_deleted' => $emptySetsDeleted,
            ];
        });
    }

    /**
     * Process a batch of tags using pipeline.
     *
     * @param \Redis|\RedisCluster $client
     * @param array<string> $tagKeys Full tag key names (with prefix)
     * @return array{scanned: int, removed: int, deleted: int}
     */
    private function processBatchPipeline(mixed $client, array $tagKeys, int $now): array
    {
        $scanned = count($tagKeys);

        // Step 1: Remove stale entries from all tags in batch
        $pipeline = $client->pipeline();
        foreach ($tagKeys as $tagKey) {
            $pipeline->zRemRangeByScore($tagKey, '0', (string) $now);
        }
        $removeResults = $pipeline->exec();

        $removed = 0;
        foreach ($removeResults as $result) {
            $removed += is_int($result) ? $result : 0;
        }

        // Step 2: Check cardinality of all tags
        $pipeline = $client->pipeline();
        foreach ($tagKeys as $tagKey) {
            $pipeline->zCard($tagKey);
        }
        $cardResults = $pipeline->exec();

        // Step 3: Delete empty sets
        $emptyKeys = [];
        foreach ($cardResults as $i => $count) {
            if ($count === 0) {
                $emptyKeys[] = $tagKeys[$i];
            }
        }

        $deleted = 0;
        if (! empty($emptyKeys)) {
            $pipeline = $client->pipeline();
            foreach ($emptyKeys as $key) {
                $pipeline->del($key);
            }
            $pipeline->exec();
            $deleted = count($emptyKeys);
        }

        return [
            'scanned' => $scanned,
            'removed' => $removed,
            'deleted' => $deleted,
        ];
    }

    /**
     * Scan for tag sorted set keys matching the pattern.
     *
     * For standard Redis, scans the single instance.
     * For RedisCluster, scans ALL master nodes since keys are distributed.
     *
     * @param \Redis|\RedisCluster $client
     * @return \Generator<string> Yields full tag key names (with prefix)
     */
    private function scanForTags(mixed $client, string $pattern, int $count): \Generator
    {
        $seen = [];

        if ($client instanceof RedisCluster) {
            // Cluster mode: scan each master node to find all keys
            // RedisCluster::scan() requires a node parameter
            foreach ($client->_masters() as $master) {
                yield from $this->scanNode($client, $master, $pattern, $count, $seen);
            }
        } else {
            // Standard mode: scan single instance
            yield from $this->scanNode($client, null, $pattern, $count, $seen);
        }
    }

    /**
     * Scan a single Redis node for keys matching the pattern.
     *
     * @param \Redis|\RedisCluster $client
     * @param array|null $node Master node address for cluster mode [host, port], null for standard mode
     * @param array<string, bool> $seen Reference to seen keys for deduplication
     * @return \Generator<string> Yields full tag key names (with prefix)
     */
    private function scanNode(mixed $client, ?array $node, string $pattern, int $count, array &$seen): \Generator
    {
        // phpredis 6.1.0+ uses null as initial cursor, older versions use 0
        $iterator = match (true) {
            version_compare(phpversion('redis') ?: '0', '6.1.0', '>=') => null,
            default => 0,
        };

        do {
            if ($node !== null) {
                // RedisCluster::scan(&$iterator, $key_or_address, $pattern, $count)
                $keys = $client->scan($iterator, $node, $pattern, $count);
            } else {
                // Redis::scan(&$iterator, $pattern, $count)
                $keys = $client->scan($iterator, $pattern, $count);
            }

            if ($keys === false || ! is_array($keys)) {
                break;
            }

            foreach ($keys as $key) {
                // Deduplicate (SCAN can return duplicates, especially across nodes)
                if (isset($seen[$key])) {
                    continue;
                }
                $seen[$key] = true;
                yield $key;
            }
        } while ($iterator > 0);
    }
}
