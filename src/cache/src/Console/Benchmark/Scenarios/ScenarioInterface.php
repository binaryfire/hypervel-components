<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Benchmark\Scenarios;

use Hypervel\Cache\Console\Benchmark\BenchmarkContext;
use Hypervel\Cache\Console\Benchmark\ScenarioResult;

/**
 * Interface for benchmark scenarios.
 */
interface ScenarioInterface
{
    /**
     * Get the scenario name for display.
     */
    public function name(): string;

    /**
     * Run the scenario and return results.
     */
    public function run(BenchmarkContext $ctx): ScenarioResult;
}
