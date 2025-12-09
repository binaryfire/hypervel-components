<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Support;

use Hypervel\Redis\RedisConnection;
use Redis;

/**
 * Handles serialization for cache values.
 *
 * This class centralizes the serialization logic needed for both
 * regular Redis operations and Lua script ARGV parameters.
 */
class Serialization
{
    public function __construct(
        private readonly StoreContext $context,
    ) {}

    /**
     * Serialize a value for storage in Redis.
     *
     * When a serializer is configured on the connection, returns the raw value
     * (phpredis will auto-serialize). Otherwise, uses PHP serialization.
     */
    public function serialize(mixed $value): mixed
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($value) {
            if ($conn->serialized()) {
                return $value;
            }

            return $this->phpSerialize($value);
        });
    }

    /**
     * Unserialize a value retrieved from Redis.
     *
     * When a serializer is configured on the connection, returns the value as-is
     * (phpredis already unserialized it). Otherwise, uses PHP unserialization.
     */
    public function unserialize(mixed $value): mixed
    {
        if ($value === null || $value === false) {
            return null;
        }

        return $this->context->withConnection(function (RedisConnection $conn) use ($value) {
            if ($conn->serialized()) {
                return $value;
            }

            return $this->phpUnserialize($value);
        });
    }

    /**
     * Serialize a value for use in Lua script ARGV.
     *
     * Unlike regular serialization (which returns raw values when a serializer
     * is configured, expecting phpredis to auto-serialize), Lua scripts require
     * pre-serialized string values in ARGV because phpredis does NOT auto-serialize
     * Lua ARGV parameters.
     *
     * This method handles three scenarios:
     * 1. Serializer configured (igbinary/json/php): Use pack() which calls _serialize()
     * 2. No serializer, but compression enabled: PHP serialize, then compress
     * 3. No serializer, no compression: Just PHP serialize
     */
    public function serializeForLua(mixed $value): string
    {
        return $this->context->withConnection(function (RedisConnection $conn) use ($value) {
            // Case 1: Serializer configured (e.g. igbinary/json)
            // pack() calls _serialize() which handles serialization AND compression
            if ($conn->serialized()) {
                return $conn->pack([$value])[0];
            }

            // No serializer - must PHP-serialize first
            $serialized = $this->phpSerialize($value);

            // Case 2: Check if compression is enabled (even without serializer)
            $client = $conn->client();

            if ($client->getOption(Redis::OPT_COMPRESSION) !== Redis::COMPRESSION_NONE) {
                // _serialize() applies compression even with SERIALIZER_NONE
                // Cast to string in case serialize() returned a numeric value
                return $client->_serialize(is_numeric($serialized) ? (string) $serialized : $serialized);
            }

            // Case 3: No serializer, no compression
            // Cast to string in case serialize() returned a numeric value
            return is_numeric($serialized) ? (string) $serialized : $serialized;
        });
    }

    /**
     * PHP serialize a value (Laravel's default logic).
     *
     * Returns raw numeric values for performance optimization.
     */
    private function phpSerialize(mixed $value): mixed
    {
        // is_nan() only works on floats, so check is_float first
        return is_numeric($value) && ! in_array($value, [INF, -INF]) && ! (is_float($value) && is_nan($value))
            ? $value
            : serialize($value);
    }

    /**
     * PHP unserialize a value.
     */
    private function phpUnserialize(mixed $value): mixed
    {
        return is_numeric($value) ? $value : unserialize((string) $value);
    }
}
