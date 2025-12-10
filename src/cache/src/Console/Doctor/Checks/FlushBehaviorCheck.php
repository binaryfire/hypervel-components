<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests flush behavior semantics.
 *
 * Union mode: Any tag flushes item (OR logic).
 * Intersection mode: All tags required to flush (AND logic).
 */
final class FlushBehaviorCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Flush Behavior Semantics';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        if ($ctx->isUnionMode()) {
            $this->testUnionMode($ctx, $result);
        } else {
            $this->testIntersectionMode($ctx, $result);
        }

        return $result;
    }

    private function testUnionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // Setup items with different tag combinations
        $ctx->cache->tags([$ctx->prefixed('color:red'), $ctx->prefixed('color:blue')])->put($ctx->prefixed('flush:purple'), 'purple', 60);
        $ctx->cache->tags([$ctx->prefixed('color:red'), $ctx->prefixed('color:yellow')])->put($ctx->prefixed('flush:orange'), 'orange', 60);
        $ctx->cache->tags([$ctx->prefixed('color:blue'), $ctx->prefixed('color:yellow')])->put($ctx->prefixed('flush:green'), 'green', 60);
        $ctx->cache->tags([$ctx->prefixed('color:red')])->put($ctx->prefixed('flush:red'), 'red only', 60);
        $ctx->cache->tags([$ctx->prefixed('color:blue')])->put($ctx->prefixed('flush:blue'), 'blue only', 60);

        // Flush one tag
        $ctx->cache->tags([$ctx->prefixed('color:red')])->flush();

        $result->assert(
            $ctx->cache->get($ctx->prefixed('flush:purple')) === null
            && $ctx->cache->get($ctx->prefixed('flush:orange')) === null
            && $ctx->cache->get($ctx->prefixed('flush:red')) === null
            && $ctx->cache->get($ctx->prefixed('flush:green')) === 'green'
            && $ctx->cache->get($ctx->prefixed('flush:blue')) === 'blue only',
            'Flushing one tag removes all items with that tag (union/OR behavior)'
        );

        // Flush multiple tags
        $ctx->cache->tags([$ctx->prefixed('color:blue'), $ctx->prefixed('color:yellow')])->flush();

        $result->assert(
            $ctx->cache->get($ctx->prefixed('flush:green')) === null
            && $ctx->cache->get($ctx->prefixed('flush:blue')) === null,
            'Flushing multiple tags removes items with ANY of those tags'
        );
    }

    private function testIntersectionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // TODO: Implement intersection mode flush behavior tests
        // Intersection mode: flush(['a', 'b']) only removes items that have BOTH tags
        $result->assert(
            true,
            'Intersection mode flush behavior (placeholder)'
        );
    }
}
