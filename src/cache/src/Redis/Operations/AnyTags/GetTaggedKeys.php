<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Operations\AnyTags;

use Generator;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;

/**
 * Get all keys associated with a tag.
 *
 * Uses adaptive scanning strategy based on hash size:
 * - At or below threshold: Uses HKEYS (faster, loads all into memory)
 * - Above threshold: Uses HSCAN (memory-efficient streaming)
 */
class GetTaggedKeys
{
    /**
     * Default threshold for switching from HKEYS to HSCAN.
     * Above this number of fields, use HSCAN for memory efficiency.
     */
    private const DEFAULT_SCAN_THRESHOLD = 1000;

    /**
     * Create a new get tagged keys query instance.
     */
    public function __construct(
        private readonly StoreContext $context,
        private readonly int $scanThreshold = self::DEFAULT_SCAN_THRESHOLD,
    ) {}

    /**
     * Execute the query.
     *
     * @param string $tag The tag name
     * @param int $count HSCAN count parameter (items per iteration)
     * @return Generator<string> Generator yielding cache keys (without prefix)
     */
    public function execute(string $tag, int $count = 1000): Generator
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($tag, $count) {
            $client = $conn->client();
            $tagKey = $this->context->tagHashKey($tag);

            // For small hashes, just get all at once
            $size = $client->hlen($tagKey);

            if ($size <= $this->scanThreshold) {
                $fields = $client->hkeys($tagKey);

                return $this->arrayToGenerator($fields ?: []);
            }

            // For large hashes, use HSCAN with Generator
            return $this->hscanGenerator($client, $tagKey, $count);
        });
    }

    /**
     * Convert an array to a generator.
     *
     * @param array<string> $items
     * @return Generator<string>
     */
    private function arrayToGenerator(array $items): Generator
    {
        foreach ($items as $item) {
            yield $item;
        }
    }

    /**
     * Create a generator using HSCAN for memory-efficient iteration.
     *
     * @param \Redis|\RedisCluster $client
     * @return Generator<string>
     */
    private function hscanGenerator(mixed $client, string $tagKey, int $count): Generator
    {
        $iterator = null;

        do {
            // phpredis: Pass iterator by reference
            $fields = $client->hscan($tagKey, $iterator, null, $count);

            if ($fields !== false && ! empty($fields)) {
                // HSCAN returns key-value pairs, we only need keys
                foreach (array_keys($fields) as $key) {
                    yield $key;
                }
            }
        } while ($iterator > 0);
    }
}
