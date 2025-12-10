<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests performance with large datasets (500+ items).
 *
 * This check is mode-agnostic.
 */
final class LargeDatasetCheck implements CheckInterface
{
    private const ITEM_COUNT = 500;

    public function name(): string
    {
        return 'Large Dataset Operations';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();
        $count = self::ITEM_COUNT;

        // Bulk insert
        $startTime = microtime(true);

        for ($i = 0; $i < $count; $i++) {
            $ctx->cache->tags([$ctx->prefixed('large-set')])->put($ctx->prefixed("large:item{$i}"), "value{$i}", 60);
        }

        $insertTime = microtime(true) - $startTime;

        $result->assert(
            $ctx->cache->get($ctx->prefixed('large:item0')) === 'value0'
            && $ctx->cache->get($ctx->prefixed('large:item' . ($count - 1))) === 'value' . ($count - 1),
            "Inserted {$count} items (took " . number_format($insertTime, 2) . 's)'
        );

        // Bulk flush
        $startTime = microtime(true);
        $ctx->cache->tags([$ctx->prefixed('large-set')])->flush();
        $flushTime = microtime(true) - $startTime;

        $result->assert(
            $ctx->cache->get($ctx->prefixed('large:item0')) === null
            && $ctx->cache->get($ctx->prefixed('large:item' . ($count - 1))) === null,
            "Flushed {$count} items (took " . number_format($flushTime, 2) . 's)'
        );

        return $result;
    }
}
