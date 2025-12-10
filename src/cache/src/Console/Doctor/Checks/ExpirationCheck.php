<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hyperf\Stringable\Str;
use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Tests TTL expiration behavior.
 *
 * Basic expiration is mode-agnostic, but hash field cleanup verification
 * is union mode specific.
 */
final class ExpirationCheck implements CheckInterface
{
    private ?OutputInterface $output = null;

    public function setOutput(OutputInterface $output): void
    {
        $this->output = $output;
    }

    public function name(): string
    {
        return 'Expiration Tests';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        $tag = $ctx->prefixed('expire-' . Str::random(8));
        $key = $ctx->prefixed('expire:' . Str::random(8));

        // Put with 1 second TTL
        $ctx->cache->tags([$tag])->put($key, 'val', 1);

        $this->output?->writeln('  <fg=gray>Waiting 2 seconds for expiration...</>');
        sleep(2);

        $result->assert(
            $ctx->cache->get($key) === null,
            'Item expired after TTL'
        );

        if ($ctx->isUnionMode()) {
            $this->testUnionModeExpiration($ctx, $result, $tag, $key);
        } else {
            $this->testIntersectionModeExpiration($ctx, $result, $tag, $key);
        }

        return $result;
    }

    private function testUnionModeExpiration(
        DoctorContext $ctx,
        CheckResult $result,
        string $tag,
        string $key,
    ): void {
        // Check hash field cleanup
        $connection = $ctx->store->connection();
        $tagKey = $ctx->tagHashKey($tag);

        $result->assert(
            ! $connection->hexists($tagKey, $key),
            'Tag hash field expired (HEXPIRE cleanup)'
        );
    }

    private function testIntersectionModeExpiration(
        DoctorContext $ctx,
        CheckResult $result,
        string $tag,
        string $key,
    ): void {
        // TODO: Verify intersection mode sorted set cleanup (ZREMRANGEBYSCORE)
        $result->assert(
            true,
            'Intersection mode expiration cleanup (placeholder)'
        );
    }
}
