<?php

declare(strict_types=1);

namespace Hypervel\Permission\Traits;

use BackedEnum;
use Hyperf\Collection\Collection as BaseCollection;
use Hyperf\Database\Model\Builder;
use Hyperf\Database\Model\Relations\MorphToMany;
use Hypervel\Database\Eloquent\Collection;
use Hypervel\Permission\Contracts\Role;
use Hypervel\Permission\PermissionManager;
use InvalidArgumentException;
use UnitEnum;

/**
 * Trait HasRole.
 *
 * This trait provides methods to check if a owner has a specific role
 * and to retrieve all roles assigned to the owner.
 *
 * @property-read Collection<Role> $roles
 */
trait HasRole
{
    use HasPermission;

    private ?string $roleClass = null;

    public function getRoleClass(): string
    {
        if ($this->roleClass === null) {
            $this->roleClass = app(PermissionManager::class)->getRoleClass();
        }

        return $this->roleClass;
    }

    /**
     * Get PermissionManager instance.
     */
    protected function getPermissionManager(): PermissionManager
    {
        return app(PermissionManager::class);
    }

    /**
     * Get owner type for cache key generation.
     */
    protected function getOwnerType(): string
    {
        return static::class;
    }

    /**
     * Get cached or fresh roles for this owner.
     */
    protected function getCachedRoles(): Collection
    {
        $manager = $this->getPermissionManager();
        $cachedRoles = $manager->getOwnerCachedRoles($this->getOwnerType(), $this->getKey());

        if ($cachedRoles !== null && ! empty($cachedRoles)) {
            // Convert cached data back to models
            /* @phpstan-ignore-next-line */
            return $this->roles()->getRelated()->hydrate($cachedRoles);
        }

        // Load from database and cache
        $this->loadMissing('roles');
        $roles = $this->roles;

        // Cache the roles data
        $manager->cacheOwnerRoles(
            $this->getOwnerType(),
            $this->getKey(),
            $roles->toArray()
        );

        return $roles;
    }

    /**
     * A owner may have multiple roles.
     */
    public function roles(): MorphToMany
    {
        return $this->morphToMany(
            $this->getRoleClass(),
            config('permission.table_names.owner_name', 'owner'),
            config('permission.table_names.owner_has_roles', 'owner_has_roles'),
            config('permission.column_names.owner_morph_key', 'owner_id'),
            config('permission.column_names.role_pivot_key', 'role_id')
        );
    }

    /**
     * Check if the owner has a specific role.
     */
    public function hasRole(BackedEnum|int|string|UnitEnum $role): bool
    {
        $roles = $this->getCachedRoles();

        [$field, $value] = $this->normalizeRoleValue($role);

        return $roles->contains($field, $value);
    }

    /**
     * Normalize role value to field and value pair.
     */
    private function normalizeRoleValue(BackedEnum|int|string|UnitEnum $role): array
    {
        $value = $this->extractRoleValue($role);
        $isId = $this->isRoleIdType($role);

        return $isId
            ? [(new ($this->getRoleClass())())->getKeyName(), $value]
            : ['name', $value];
    }

    /**
     * Extract the actual value from a role of any supported type.
     */
    private function extractRoleValue(BackedEnum|int|string|UnitEnum $role): int|string
    {
        return match (true) {
            $role instanceof BackedEnum => $role->value,
            $role instanceof UnitEnum => $role->name,
            default => $role
        };
    }

    /**
     * Check if the role should be treated as an ID (int) rather than name (string).
     *
     * @throws InvalidArgumentException if the role type is unsupported
     */
    private function isRoleIdType(BackedEnum|int|string|UnitEnum $role): bool
    {
        return match (true) {
            is_int($role) => true,
            $role instanceof BackedEnum => is_int($role->value),
            is_string($role), $role instanceof UnitEnum => false,
            default => throw new InvalidArgumentException('Invalid role type')
        };
    }

    /**
     * Separate roles array into IDs and names collections.
     *
     * @param array<int, BackedEnum|int|string|UnitEnum> $roles
     */
    private function separateRolesByType(array $roles): array
    {
        $roleIds = BaseCollection::make();
        $roleNames = BaseCollection::make();

        foreach ($roles as $role) {
            $value = $this->extractRoleValue($role);

            if ($this->isRoleIdType($role)) {
                $roleIds->push($value);
            } else {
                $roleNames->push($value);
            }
        }

        return [$roleIds, $roleNames];
    }

    /**
     * Check if the owner has any of the specified roles.
     *
     * @param array<int, BackedEnum|int|string|UnitEnum> $roles
     */
    public function hasAnyRoles(array $roles): bool
    {
        $ownerRoles = $this->getCachedRoles();

        return BaseCollection::make($roles)->some(function ($role) use ($ownerRoles) {
            [$field, $value] = $this->normalizeRoleValue($role);

            return $ownerRoles->contains($field, $value);
        });
    }

    /**
     * Check if the owner has all of the specified roles.
     *
     * @param array<int, BackedEnum|int|string|UnitEnum> $roles
     */
    public function hasAllRoles(array $roles): bool
    {
        $ownerRoles = $this->getCachedRoles();

        return BaseCollection::make($roles)->every(function ($role) use ($ownerRoles) {
            [$field, $value] = $this->normalizeRoleValue($role);

            return $ownerRoles->contains($field, $value);
        });
    }

    /**
     * Get only the roles that match the specified roles from the owner's assigned roles.
     *
     * @param array<int, BackedEnum|int|string|UnitEnum> $roles
     */
    public function onlyRoles(array $roles): Collection
    {
        $ownerRoles = $this->getCachedRoles();

        [$inputRoleIds, $inputRoleNames] = $this->separateRolesByType($roles);

        $keyName = (new ($this->getRoleClass())())->getKeyName();
        $currentRoleIds = $ownerRoles->pluck($keyName);
        $currentRoleNames = $ownerRoles->pluck('name');

        $intersectedIds = $currentRoleIds->intersect($inputRoleIds);
        $intersectedNames = $currentRoleNames->intersect($inputRoleNames);

        return $ownerRoles->filter(
            /* @phpstan-ignore-next-line */
            fn ($role) => $intersectedIds->contains($role->{$keyName}) || $intersectedNames->contains($role->name)
        )
            ->values();
    }

    /**
     * Assign roles to the owner.
     */
    public function assignRole(array|BackedEnum|int|string|UnitEnum ...$roles): static
    {
        $this->loadMissing('roles');
        $roles = $this->collectRoles($roles);

        $keyName = (new ($this->getRoleClass())())->getKeyName();
        $currentRoles = $this->roles->map(fn ($role) => $role->{$keyName})->toArray();
        $this->roles()->attach(array_diff($roles, $currentRoles));

        $this->unsetRelation('roles');

        // Clear owner cache when roles are modified
        $this->getPermissionManager()->clearOwnerCache($this->getOwnerType(), $this->getKey());

        return $this;
    }

    /**
     * Revoke the given role from owner.
     */
    public function removeRole(array|BackedEnum|int|string|UnitEnum ...$roles): static
    {
        $detachRoles = $this->collectRoles($roles);

        $this->roles()->detach($detachRoles);

        // Clear owner cache when roles are modified
        $this->getPermissionManager()->clearOwnerCache($this->getOwnerType(), $this->getKey());

        return $this;
    }

    /**
     * Synchronize the owner's roles with the given role list.
     */
    public function syncRoles(array|BackedEnum|int|string|UnitEnum ...$roles): array
    {
        $roles = $this->collectRoles($roles);

        $result = $this->roles()->sync($roles);

        // Clear owner cache when roles are modified
        $this->getPermissionManager()->clearOwnerCache($this->getOwnerType(), $this->getKey());

        return $result;
    }

    /**
     * Returns array of role ids.
     */
    private function collectRoles(array|BackedEnum|int|string|UnitEnum ...$roles): array
    {
        $roles = BaseCollection::make($roles)
            ->flatten()
            ->values()
            ->all();
        [$roleIds, $roleNames] = $this->separateRolesByType($roles);

        $roleInstance = new ($this->getRoleClass())();
        $keyName = $roleInstance->getKeyName();
        $query = $roleInstance::query();
        $query->where(function (Builder $query) use ($keyName, $roleIds, $roleNames) {
            if ($roleIds->isNotEmpty()) {
                $query->orWhereIn($keyName, $roleIds);
            }

            if ($roleNames->isNotEmpty()) {
                $query->orWhereIn('name', $roleNames);
            }
        });

        return $query->pluck('id')->toArray();
    }
}
