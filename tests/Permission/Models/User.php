<?php

declare(strict_types=1);

namespace Hypervel\Tests\Permission\Models;

use Hypervel\Auth\Access\Authorizable;
use Hypervel\Auth\Authenticatable;
use Hypervel\Auth\Contracts\Authenticatable as AuthenticatableContract;
use Hypervel\Auth\Contracts\Authorizable as AuthorizableContract;
use Hypervel\Database\Eloquent\Model;
use Hypervel\Permission\Traits\HasRole;

class User extends Model implements AuthenticatableContract, AuthorizableContract
{
    use Authenticatable;
    use Authorizable;
    use HasRole;

    protected array $guarded = [];
}
