<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests rapid sequential operations.
 *
 * This check is mode-agnostic.
 */
final class SequentialOperationsCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Sequential Rapid Operations';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Rapid writes to same key
        for ($i = 0; $i < 10; $i++) {
            $ctx->cache->tags([$ctx->prefixed('rapid')])->put($ctx->prefixed('concurrent:key'), "value{$i}", 60);
        }

        $result->assert(
            $ctx->cache->get($ctx->prefixed('concurrent:key')) === 'value9',
            'Last write wins in rapid succession'
        );

        // Multiple increments
        $ctx->cache->put($ctx->prefixed('concurrent:counter'), 0, 60);

        for ($i = 0; $i < 50; $i++) {
            $ctx->cache->increment($ctx->prefixed('concurrent:counter'));
        }

        $result->assert(
            $ctx->cache->get($ctx->prefixed('concurrent:counter')) === '50',
            'Multiple increments all applied correctly'
        );

        // Race condition: add operations
        $ctx->cache->forget($ctx->prefixed('concurrent:add'));
        $results = [];

        for ($i = 0; $i < 5; $i++) {
            $results[] = $ctx->cache->add($ctx->prefixed('concurrent:add'), "value{$i}", 60);
        }

        $result->assert(
            $results[0] === true && array_sum($results) === 1,
            'add() is atomic (only first succeeds)'
        );

        // Overlapping tag operations
        $ctx->cache->tags([$ctx->prefixed('overlap1'), $ctx->prefixed('overlap2')])->put($ctx->prefixed('concurrent:overlap'), 'value', 60);
        $ctx->cache->tags([$ctx->prefixed('overlap1')])->flush();
        $result->assert(
            $ctx->cache->get($ctx->prefixed('concurrent:overlap')) === null,
            'Partial flush removes item correctly'
        );

        return $result;
    }
}
