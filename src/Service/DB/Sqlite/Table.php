<?php
declare(strict_types=1);

namespace Owl\Service\DB\Sqlite;

// @FIXME
class Table extends \Owl\Service\DB\Table
{
    protected function listColumns(): array
    {
        return [];
    }

    protected function listIndexes(): array
    {
        return [];
    }

    protected function listForeignKeys(): array
    {
        return [];
    }
}
