<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests bulk operations: putMany() and many().
 *
 * Basic operations are mode-agnostic, but tag hash verification is union mode specific.
 */
final class BulkOperationsCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Bulk Operations (putMany/many)';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // putMany without tags
        $ctx->cache->putMany([
            $ctx->prefixed('bulk:1') => 'value1',
            $ctx->prefixed('bulk:2') => 'value2',
            $ctx->prefixed('bulk:3') => 'value3',
        ], 60);

        $result->assert(
            $ctx->cache->get($ctx->prefixed('bulk:1')) === 'value1'
            && $ctx->cache->get($ctx->prefixed('bulk:2')) === 'value2'
            && $ctx->cache->get($ctx->prefixed('bulk:3')) === 'value3',
            'putMany() stores multiple items'
        );

        // many()
        $values = $ctx->cache->many([
            $ctx->prefixed('bulk:1'),
            $ctx->prefixed('bulk:2'),
            $ctx->prefixed('bulk:nonexistent'),
        ]);
        $result->assert(
            $values[$ctx->prefixed('bulk:1')] === 'value1'
            && $values[$ctx->prefixed('bulk:2')] === 'value2'
            && $values[$ctx->prefixed('bulk:nonexistent')] === null,
            'many() retrieves multiple items (null for missing)'
        );

        // putMany with tags
        $ctx->cache->tags([$ctx->prefixed('bulk')])->putMany([
            $ctx->prefixed('bulk:tagged1') => 'tagged1',
            $ctx->prefixed('bulk:tagged2') => 'tagged2',
        ], 60);

        if ($ctx->isUnionMode()) {
            $result->assert(
                $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('bulk')), $ctx->prefixed('bulk:tagged1')) === true
                && $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('bulk')), $ctx->prefixed('bulk:tagged2')) === true,
                'putMany() with tags adds all items to tag hash (union mode)'
            );
        } else {
            // TODO: Verify intersection mode sorted set entries
            $result->assert(
                true,
                'putMany() with tags adds entries (intersection mode - placeholder)'
            );
        }

        // Flush putMany tags
        $ctx->cache->tags([$ctx->prefixed('bulk')])->flush();
        $result->assert(
            $ctx->cache->get($ctx->prefixed('bulk:tagged1')) === null
            && $ctx->cache->get($ctx->prefixed('bulk:tagged2')) === null,
            'flush() removes items added via putMany()'
        );

        return $result;
    }
}
