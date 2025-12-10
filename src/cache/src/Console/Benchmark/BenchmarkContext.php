<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Benchmark;

use Exception;
use Hyperf\Command\Command;
use Hypervel\Cache\Contracts\Factory as CacheContract;
use Hypervel\Cache\Contracts\Repository;
use Hypervel\Cache\Exceptions\BenchmarkMemoryException;
use Hypervel\Cache\Redis\TagMode;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\Support\SystemInfo;
use RuntimeException;
use Symfony\Component\Console\Helper\ProgressBar;

/**
 * Context object that bundles shared state for benchmark scenarios.
 */
class BenchmarkContext
{
    /**
     * Key prefix for benchmark-generated cache entries.
     * Matches the pattern used by DoctorContext (_doctor:test:).
     */
    public const KEY_PREFIX = '_bench:';

    /**
     * Memory usage threshold (percentage) before throwing exception.
     */
    private int $memoryThreshold = 85;

    /**
     * Cached store instance.
     */
    private ?RedisStore $storeInstance = null;

    /**
     * Cached cache prefix.
     */
    private ?string $cachePrefix = null;

    /**
     * Create a new benchmark context instance.
     */
    public function __construct(
        public readonly string $storeName,
        public readonly int $items,
        public readonly int $tagsPerItem,
        public readonly int $heavyTags,
        public readonly Command $command,
        private readonly CacheContract $cacheManager,
    ) {}

    /**
     * Get the cache repository for this context.
     */
    public function getStore(): Repository
    {
        return $this->cacheManager->store($this->storeName);
    }

    /**
     * Get the underlying store instance.
     */
    public function getStoreInstance(): RedisStore
    {
        if ($this->storeInstance !== null) {
            return $this->storeInstance;
        }

        $store = $this->getStore()->getStore();

        if (! $store instanceof RedisStore) {
            throw new RuntimeException(
                'Benchmark requires a Redis store, but got: ' . $store::class
            );
        }

        return $this->storeInstance = $store;
    }

    /**
     * Get the cache prefix.
     */
    public function getCachePrefix(): string
    {
        return $this->cachePrefix ??= $this->getStoreInstance()->getPrefix();
    }

    /**
     * Get the current tag mode.
     */
    public function getTagMode(): TagMode
    {
        return $this->getStoreInstance()->getTagMode();
    }

    /**
     * Check if the store is configured for 'any' tag mode.
     */
    public function isAnyMode(): bool
    {
        return $this->getTagMode() === TagMode::Any;
    }

    /**
     * Check if the store is configured for 'all' tag mode.
     */
    public function isAllMode(): bool
    {
        return $this->getTagMode() === TagMode::All;
    }

    /**
     * Get a pattern to match all tag storage structures with a given tag name prefix.
     *
     * Uses TagMode to build correct pattern for current mode:
     * - Any mode: {cachePrefix}_any:tag:{tagNamePrefix}*
     * - All mode: {cachePrefix}_all:tag:{tagNamePrefix}*
     *
     * @param string $tagNamePrefix The prefix to match tag names against
     * @return string The pattern to use with SCAN/KEYS commands
     */
    public function getTagStoragePattern(string $tagNamePrefix): string
    {
        $tagMode = $this->getTagMode();

        return $this->getCachePrefix() . $tagMode->tagSegment() . $tagNamePrefix . '*';
    }

    /**
     * Get patterns to match all cache value keys with a given key prefix.
     *
     * Returns an array because all mode needs multiple patterns:
     * - Untagged keys: {cachePrefix}{keyPrefix}* (same in both modes)
     * - Tagged keys in all mode: {cachePrefix}{sha1}:{keyPrefix}* (namespaced)
     *
     * @param string $keyPrefix The prefix to match cache keys against
     * @return array<string> Patterns to use with SCAN/KEYS commands
     */
    public function getCacheValuePatterns(string $keyPrefix): array
    {
        $prefix = $this->getCachePrefix();

        // Untagged cache values are always at {cachePrefix}{keyName} in both modes
        $patterns = [$prefix . $keyPrefix . '*'];

        if ($this->isAllMode()) {
            // All mode also has tagged values at {cachePrefix}{sha1}:{keyName}
            $patterns[] = $prefix . '*:' . $keyPrefix . '*';
        }

        return $patterns;
    }

    /**
     * Create a key with the benchmark prefix.
     */
    public function key(string $suffix): string
    {
        return self::KEY_PREFIX . $suffix;
    }

    /**
     * Create a progress bar using the command's output style.
     */
    public function createProgressBar(int $max): ProgressBar
    {
        return $this->command->getOutput()->createProgressBar($max);
    }

    /**
     * Write a line to output.
     */
    public function line(string $message): void
    {
        $this->command->line($message);
    }

    /**
     * Write a blank line to output.
     */
    public function newLine(int $count = 1): void
    {
        $this->command->newLine($count);
    }

    /**
     * Call another command (with output).
     */
    public function call(string $command, array $arguments = []): int
    {
        return $this->command->call($command, $arguments);
    }

    /**
     * Check memory usage and throw exception if approaching limit.
     *
     * @throws BenchmarkMemoryException
     */
    public function checkMemoryUsage(): void
    {
        $currentUsage = memory_get_usage(true);
        $memoryLimit = (new SystemInfo())->getMemoryLimitBytes();

        if ($memoryLimit === -1) {
            return;
        }

        $usagePercent = (int) (($currentUsage / $memoryLimit) * 100);

        if ($usagePercent >= $this->memoryThreshold) {
            throw new BenchmarkMemoryException($currentUsage, $memoryLimit, $usagePercent);
        }
    }

    /**
     * Perform cleanup of benchmark data.
     *
     * This method uses mode-aware patterns to ensure complete cleanup:
     * 1. Flush all tagged items via $store->tags()->flush()
     * 2. Clean non-tagged benchmark keys
     * 3. Clean any remaining tag storage structures
     * 4. Run prune command to clean up orphans
     */
    public function cleanup(): void
    {
        $store = $this->getStore();
        $storeInstance = $this->getStoreInstance();

        // Build list of all benchmark tags
        $tags = [
            'deep:tag',
            'read:tag',
            'bulk:tag',
            'cleanup:main',
            'cleanup:shared:1',
            'cleanup:shared:2',
            'cleanup:shared:3',
        ];

        // Standard tags (max 10)
        for ($i = 0; $i < 10; $i++) {
            $tags[] = "tag:{$i}";
        }

        // Heavy tags (max 60 to cover extreme scale)
        for ($i = 0; $i < 60; $i++) {
            $tags[] = "heavy:tag:{$i}";
        }

        // 1. Flush tagged items - this handles cache values, tag hashes/zsets, and registry
        $store->tags($tags)->flush();

        // 2. Clean up non-tagged benchmark keys using mode-aware patterns
        // In all mode, tagged keys are at {prefix}{sha1}:{key}, so we need multiple patterns
        foreach ($this->getCacheValuePatterns(self::KEY_PREFIX) as $pattern) {
            $this->flushKeysByPattern($storeInstance, $pattern);
        }

        // 3. Clean up any remaining tag storage structures (belt and suspenders)
        // This catches any leftover tag hashes/zsets that weren't flushed
        $tagStoragePattern = $this->getTagStoragePattern('');
        $this->flushKeysByPattern($storeInstance, $tagStoragePattern);

        // 4. Run prune command to clean up any orphans
        try {
            $this->call('cache:prune-stale-tags', ['store' => $this->storeName]);
        } catch (Exception) {
            // Ignore if command fails
        }
    }

    /**
     * Flush keys by pattern using SCAN + UNLINK.
     *
     * @param RedisStore $store The Redis store instance
     * @param string $pattern The pattern to match (should include cache prefix)
     */
    private function flushKeysByPattern(RedisStore $store, string $pattern): void
    {
        $context = $store->getContext();

        $context->withConnection(function ($conn) use ($pattern) {
            // Access raw Redis client for scan() which requires pass-by-reference cursor
            $client = $conn->client();
            $cursor = null;

            do {
                // PHPRedis scan() modifies cursor by reference and returns keys array
                /** @var array<int, string>|false $keys */
                $keys = $client->scan($cursor, $pattern, 1000);

                if ($keys === false) {
                    break;
                }

                if ($keys !== []) {
                    $conn->unlink(...$keys);
                }
                // PHPRedis modifies $cursor by reference: 0 when done, >0 to continue
            } while ((int) $cursor > 0); // @phpstan-ignore greater.alwaysFalse (cursor modified by reference)
        });
    }
}
