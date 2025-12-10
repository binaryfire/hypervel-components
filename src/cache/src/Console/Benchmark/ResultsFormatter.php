<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Benchmark;

use Hyperf\Command\Command;
use Hypervel\Support\Number;

/**
 * Formats and displays benchmark results.
 */
class ResultsFormatter
{
    /**
     * The output interface.
     */
    private Command $command;

    /**
     * Metric display configuration.
     *
     * @var array<string, array{label: string, unit: string, format: string, better: string}>
     */
    private array $metricConfig = [
        // Non-Tagged Operations
        'put_rate' => ['label' => 'Non-Tagged: put()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],
        'get_rate' => ['label' => 'Non-Tagged: get()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],
        'forget_rate' => ['label' => 'Non-Tagged: forget()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],
        'remember_rate' => ['label' => 'Non-Tagged: remember()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],
        'putmany_rate' => ['label' => 'Non-Tagged: putMany()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],
        'add_rate' => ['label' => 'Non-Tagged: add()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],

        // Standard Tagging
        'write_rate' => ['label' => 'Tagged: put()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],
        'write_time' => ['label' => 'Tagged: put() Total', 'unit' => 'Seconds', 'format' => 'time', 'better' => 'lower'],
        'flush_time' => ['label' => 'Tagged: flush()', 'unit' => 'Seconds', 'format' => 'time', 'better' => 'lower'],
        'putmany_rate' => ['label' => 'Tagged: putMany()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],

        // Read Performance
        'read_rate' => ['label' => 'Tagged: get()', 'unit' => 'Items/sec', 'format' => 'rate', 'better' => 'higher'],

        // Cleanup
        'cleanup_time' => ['label' => 'Cleanup Command', 'unit' => 'Seconds', 'format' => 'time', 'better' => 'lower'],
    ];

    /**
     * Create a new results formatter instance.
     */
    public function __construct(Command $command)
    {
        $this->command = $command;
    }

    /**
     * Display results table for a single mode.
     *
     * @param array<string, ScenarioResult> $results
     */
    public function displayResultsTable(array $results, string $tagMode): void
    {
        $this->command->newLine();
        $this->command->info('═══════════════════════════════════════════════════════════════');
        $this->command->info("  Results ({$tagMode} mode)");
        $this->command->info('═══════════════════════════════════════════════════════════════');
        $this->command->newLine();

        $tableData = [];

        foreach ($this->metricConfig as $metricKey => $config) {
            $value = $this->findMetricValue($results, $metricKey);

            if ($value === null) {
                continue;
            }

            $tableData[] = [
                $config['label'] . ' (' . $config['unit'] . ')',
                $this->formatValue($value, $config['format']),
            ];
        }

        $this->command->table(
            ['Metric', 'Result'],
            $tableData
        );
    }

    /**
     * Display comparison table between two tag modes.
     *
     * @param array<string, ScenarioResult> $allModeResults
     * @param array<string, ScenarioResult> $anyModeResults
     */
    public function displayComparisonTable(array $allModeResults, array $anyModeResults): void
    {
        $this->command->newLine();
        $this->command->info('═══════════════════════════════════════════════════════════════');
        $this->command->info('  Tag Mode Comparison: All vs Any');
        $this->command->info('═══════════════════════════════════════════════════════════════');
        $this->command->newLine();

        $tableData = [];

        foreach ($this->metricConfig as $metricKey => $config) {
            $allValue = $this->findMetricValue($allModeResults, $metricKey);
            $anyValue = $this->findMetricValue($anyModeResults, $metricKey);

            if ($allValue === null && $anyValue === null) {
                continue;
            }

            $diff = $this->calculateDiff($allValue, $anyValue, $config['better']);

            $tableData[] = [
                $config['label'] . ' (' . $config['unit'] . ')',
                $allValue !== null ? $this->formatValue($allValue, $config['format']) : 'N/A',
                $anyValue !== null ? $this->formatValue($anyValue, $config['format']) : 'N/A',
                $diff,
            ];
        }

        $this->command->table(
            ['Metric', 'All Mode', 'Any Mode', 'Diff'],
            $tableData
        );

        $this->displayLegend();
    }

    /**
     * Display the legend explaining color coding.
     */
    private function displayLegend(): void
    {
        $this->command->newLine();
        $this->command->line('  <fg=gray>Legend: Diff shows Any Mode relative to All Mode</>');
        $this->command->line('  <fg=green>Green (+%)</> = Any Mode is better');
        $this->command->line('  <fg=red>Red (-%)</> = Any Mode is worse');
        $this->command->line('  <fg=gray>For times, lower is better. For rates, higher is better.</>');
    }

    /**
     * Find a metric value in the results.
     *
     * @param array<string, ScenarioResult> $results
     */
    private function findMetricValue(array $results, string $metricKey): ?float
    {
        foreach ($results as $result) {
            $value = $result->get($metricKey);

            if ($value !== null) {
                return $value;
            }
        }

        return null;
    }

    /**
     * Format a value based on its type.
     */
    private function formatValue(float $value, string $format): string
    {
        return match ($format) {
            'rate' => Number::format($value, precision: 0),
            'time' => Number::format($value, precision: 4) . 's',
            default => (string) $value,
        };
    }

    /**
     * Calculate the percentage difference and format with color.
     */
    private function calculateDiff(?float $allValue, ?float $anyValue, string $better): string
    {
        if ($allValue === null || $anyValue === null || $allValue == 0) {
            return '<fg=gray>-</>';
        }

        // Calculate percentage difference: (any - all) / all * 100
        $percentDiff = (($anyValue - $allValue) / $allValue) * 100;

        // Determine if "any" mode is better
        // For rates (higher is better): positive diff = any is better
        // For times (lower is better): negative diff = any is better
        $anyIsBetter = ($better === 'higher' && $percentDiff > 0)
                    || ($better === 'lower' && $percentDiff < 0);

        $color = $anyIsBetter ? 'green' : 'red';
        $sign = $percentDiff >= 0 ? '+' : '';

        return sprintf('<fg=%s>%s%.1f%%</>', $color, $sign, $percentDiff);
    }
}
