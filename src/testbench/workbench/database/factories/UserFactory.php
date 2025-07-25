<?php

declare(strict_types=1);

use Carbon\Carbon;
use Faker\Generator as Faker;
use Workbench\App\Models\User;

/* @phpstan-ignore-next-line */
$factory->define(User::class, function (Faker $faker) {
    return [
        'name' => $faker->unique()->name(),
        'email' => $faker->unique()->safeEmail(),
        'email_verified_at' => Carbon::now(),
        'password' => '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', // password
    ];
});
