<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Benchmark\Scenarios;

use Hypervel\Cache\Console\Benchmark\BenchmarkContext;
use Hypervel\Cache\Console\Benchmark\ScenarioResult;

/**
 * Tests standard tagging with write and flush operations.
 */
class StandardTaggingScenario implements ScenarioInterface
{
    /**
     * Get the scenario name for display.
     */
    public function name(): string
    {
        return 'Standard Tagging';
    }

    /**
     * Run standard tagging benchmark with write and flush operations.
     */
    public function run(BenchmarkContext $ctx): ScenarioResult
    {
        $items = $ctx->items;
        $tagsPerItem = $ctx->tagsPerItem;

        $ctx->newLine();
        $ctx->line("  Running Standard Tagging Scenario ({$items} items, {$tagsPerItem} tags/item)...");
        $ctx->cleanup();

        // Build tags array
        $tags = [];

        for ($i = 0; $i < $tagsPerItem; $i++) {
            $tags[] = "tag:{$i}";
        }

        // 1. Write
        $ctx->line('  Testing put() with tags...');
        $start = hrtime(true);
        $bar = $ctx->createProgressBar($items);

        $store = $ctx->getStore();
        $chunkSize = 100;

        for ($i = 0; $i < $items; $i++) {
            $store->tags($tags)->put($ctx->key("item:{$i}"), 'value', 3600);

            if ($i % $chunkSize === 0) {
                $bar->advance($chunkSize);
                $ctx->checkMemoryUsage();
            }
        }

        $bar->finish();
        $ctx->line('');

        $writeTime = (hrtime(true) - $start) / 1e9;
        $writeRate = $items / $writeTime;

        // 2. Flush (Flush one tag, which should remove all items)
        $ctx->line('  Flushing items by tag...');
        $start = hrtime(true);
        $store->tags([$tags[0]])->flush();
        $flushTime = (hrtime(true) - $start) / 1e9;

        // 3. Add Performance (add)
        $ctx->cleanup();
        $ctx->line('  Testing add() with tags...');
        $start = hrtime(true);
        $bar = $ctx->createProgressBar($items);

        for ($i = 0; $i < $items; $i++) {
            $store->tags($tags)->add($ctx->key("item:add:{$i}"), 'value', 3600);

            if ($i % $chunkSize === 0) {
                $bar->advance($chunkSize);
                $ctx->checkMemoryUsage();
            }
        }

        $bar->finish();
        $ctx->line('');

        $addTime = (hrtime(true) - $start) / 1e9;
        $addRate = $items / $addTime;

        // 4. Bulk Write Performance (putMany)
        $ctx->cleanup();
        $ctx->line('  Testing putMany() with tags...');
        $bulkChunkSize = 100;
        $start = hrtime(true);
        $bar = $ctx->createProgressBar($items);

        $buffer = [];

        for ($i = 0; $i < $items; $i++) {
            $buffer[$ctx->key("item:bulk:{$i}")] = 'value';

            if (count($buffer) >= $bulkChunkSize) {
                $store->tags($tags)->putMany($buffer, 3600);
                $buffer = [];
                $bar->advance($bulkChunkSize);
            }
        }

        if (! empty($buffer)) {
            $store->tags($tags)->putMany($buffer, 3600);
            $bar->advance(count($buffer));
        }

        $bar->finish();
        $ctx->line('');

        $putManyTime = (hrtime(true) - $start) / 1e9;
        $putManyRate = $items / $putManyTime;

        return new ScenarioResult([
            'write_time' => $writeTime,
            'write_rate' => $writeRate,
            'flush_time' => $flushTime,
            'add_rate' => $addRate,
            'putmany_rate' => $putManyRate,
        ]);
    }
}
