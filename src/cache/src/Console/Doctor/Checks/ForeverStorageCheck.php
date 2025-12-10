<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests forever() storage (no expiration).
 *
 * Basic forever storage is mode-agnostic, but hash field TTL verification
 * is union mode specific.
 */
final class ForeverStorageCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Forever Storage (No Expiration)';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Forever without tags
        $ctx->cache->forever($ctx->prefixed('forever:key1'), 'permanent');
        $ttl = $ctx->redis->ttl($ctx->cachePrefix . $ctx->prefixed('forever:key1'));
        $result->assert(
            $ttl === -1,
            'forever() stores without expiration'
        );

        // Forever with tags
        $ctx->cache->tags([$ctx->prefixed('permanent')])->forever($ctx->prefixed('forever:tagged'), 'also permanent');
        $keyTtl = $ctx->redis->ttl($ctx->cachePrefix . $ctx->prefixed('forever:tagged'));

        $result->assert(
            $keyTtl === -1,
            'forever() with tags: key has no expiration'
        );

        if ($ctx->isUnionMode()) {
            $this->testUnionModeHashTtl($ctx, $result);
        } else {
            $this->testIntersectionMode($ctx, $result);
        }

        return $result;
    }

    private function testUnionModeHashTtl(DoctorContext $ctx, CheckResult $result): void
    {
        // Verify hash field also has no expiration
        $fieldTtl = $ctx->redis->httl($ctx->tagHashKey($ctx->prefixed('permanent')), [$ctx->prefixed('forever:tagged')]);
        $result->assert(
            $fieldTtl[0] === -1,
            'forever() with tags: hash field has no expiration (union mode)'
        );
    }

    private function testIntersectionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // TODO: Verify intersection mode sorted set score for forever items
        // Forever items in intersection mode have score -1
        $result->assert(
            true,
            'Intersection mode forever tag structure (placeholder)'
        );
    }
}
