<?php
declare(strict_types=1);

namespace Owl\Service\DB;

use Owl\Logger;

abstract class Adapter extends \Owl\Service
{
    protected $handler;

    protected $identifier_symbol = '`';
    protected $support_savepoint = true;
    protected $savepoints = [];
    protected $in_transaction = false;

    abstract public function lastID(string $table = null, string $column = null);

    /**
     * @return array
     */
    abstract public function getTables(): array;

    public function __construct(array $config = [])
    {
        if (!isset($config['dsn'])) {
            throw new \InvalidArgumentException('Invalid database config, require "dsn" key.');
        }
        parent::__construct($config);
    }

    public function __destruct()
    {
        if ($this->isConnected()) {
            $this->rollbackAll();
        }
    }

    public function __sleep()
    {
        $this->disconnect();
    }

    public function __call($method, array $args)
    {
        return $args
             ? call_user_func_array([$this->connect(), $method], $args)
             : $this->connect()->$method();
    }

    public function isConnected(): bool
    {
        return $this->handler instanceof \PDO;
    }

    public function connect(): \PDO
    {
        if ($this->isConnected()) {
            return $this->handler;
        }

        $dsn = $this->getConfig('dsn');
        $user = $this->getConfig('user') ?: null;
        $password = $this->getConfig('password') ?: null;
        $options = $this->getConfig('options') ?: [];

        $options[\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_EXCEPTION;

        try {
            $handler = new \PDO($dsn, $user, $password, $options);

            Logger::log('debug', 'database connected', ['dsn' => $dsn]);
        } catch (\Exception $exception) {
            Logger::log('error', 'database connect failed', [
                'error' => $exception->getMessage(),
                'dsn' => $dsn,
            ]);

            throw new \Owl\Service\Exception('Database connect failed!', 0, $exception);
        }

        return $this->handler = $handler;
    }

    public function disconnect()
    {
        if ($this->isConnected()) {
            $this->rollbackAll();
            $this->handler = null;

            Logger::log('debug', 'database disconnected', ['dsn' => $this->getConfig('dsn')]);
        }

        return $this;
    }

    public function begin(): bool
    {
        if ($this->in_transaction) {
            if (!$this->support_savepoint) {
                throw new \Exception(get_class($this) . ' unsupport savepoint');
            }

            $savepoint = $this->quoteIdentifier(uniqid('savepoint_'));
            $this->execute('SAVEPOINT ' . $savepoint);
            $this->savepoints[] = $savepoint;
        } else {
            $this->execute('BEGIN');
            $this->in_transaction = true;
        }

        return true;
    }

    public function commit(): bool
    {
        if ($this->in_transaction) {
            if ($this->savepoints) {
                $savepoint = array_pop($this->savepoints);
                $this->execute('RELEASE SAVEPOINT ' . $savepoint);
            } else {
                $this->execute('COMMIT');
                $this->in_transaction = false;
            }
        }

        return true;
    }

    public function rollback(): bool
    {
        if ($this->in_transaction) {
            if ($this->savepoints) {
                $savepoint = array_pop($this->savepoints);
                $this->execute('ROLLBACK TO SAVEPOINT ' . $savepoint);
            } else {
                $this->execute('ROLLBACK');
                $this->in_transaction = false;
            }
        }

        return true;
    }

    public function rollbackAll()
    {
        $max = 9; // 最多9次，避免死循环
        while ($this->in_transaction && $max-- > 0) {
            $this->rollback();
        }
    }

    public function inTransaction(): bool
    {
        return $this->in_transaction;
    }

    public function execute($sql, $params = null): Statement
    {
        $params = $params === null
        ? []
        : is_array($params) ? $params : array_slice(func_get_args(), 1);

        Logger::log('debug', 'database execute', [
            'sql' => ($sql instanceof \PDOStatement) ? $sql->queryString : $sql,
            'parameters' => $params,
        ]);

        if ($sql instanceof \PDOStatement || $sql instanceof Statement) {
            $sth = $sql;
            $sth->execute($params);
        } elseif ($params) {
            $sth = $this->connect()->prepare($sql);
            $sth->execute($params);
        } else {
            $sth = $this->connect()->query($sql);
        }

        $sth->setFetchMode(\PDO::FETCH_ASSOC);

        return Statement::factory($sth);
    }

    public function prepare(): Statement
    {
        $handler = $this->connect();
        $statement = call_user_func_array([$handler, 'prepare'], func_get_args());

        return Statement::factory($statement);
    }

    public function quote($value)
    {
        if (is_array($value)) {
            foreach ($value as $k => $v) {
                $value[$k] = $this->quote($v);
            }

            return $value;
        }

        if ($value instanceof Expr) {
            return $value;
        }

        if ($value === null) {
            return 'NULL';
        }

        return $this->connect()->quote($value);
    }

    public function quoteIdentifier($identifier)
    {
        if (is_array($identifier)) {
            return array_map([$this, 'quoteIdentifier'], $identifier);
        }

        if ($identifier instanceof Expr) {
            return $identifier;
        }

        $symbol = $this->identifier_symbol;
        $identifier = str_replace(['"', "'", ';', $symbol], '', $identifier);

        $result = [];
        foreach (explode('.', $identifier) as $s) {
            $result[] = $symbol . $s . $symbol;
        }

        return new Expr(implode('.', $result));
    }

    public function select($table): Select
    {
        return new Select($this, $table);
    }

    /**
     * @return \Owl\Service\DB\Table
     */
    public function getTable(string $table_name): Table
    {
        $class = str_replace('Adapter', 'Table', get_class($this));

        return new $class($this, $table_name);
    }

    public function hasTable(string $table_name): bool
    {
        $table_name = str_replace($this->identifier_symbol, '', $table_name);

        return in_array($table_name, $this->getTables());
    }

    public function insert(string $table, array $row): int
    {
        $params = [];
        foreach ($row as $value) {
            if (!($value instanceof Expr)) {
                $params[] = $value;
            }
        }

        $sth = $this->prepareInsert($table, $row);

        return $this->execute($sth, $params)->rowCount();
    }

    public function update(string $table, array $row, string $where = null, $params = null): int
    {
        $where_params = ($where === null || $params === null)
                      ? []
                      : is_array($params) ? $params : array_slice(func_get_args(), 3);

        $params = [];
        foreach ($row as $value) {
            if (!($value instanceof Expr)) {
                $params[] = $value;
            }
        }

        if ($where_params) {
            $params = array_merge($params, $where_params);
        }

        $sth = $this->prepareUpdate($table, $row, $where);

        return $this->execute($sth, $params)->rowCount();
    }

    public function delete(string $table, string $where = null, $params = null): int
    {
        $params = ($where === null || $params === null)
                ? []
                : is_array($params) ? $params : array_slice(func_get_args(), 2);

        $sth = $this->prepareDelete($table, $where);

        return $this->execute($sth, $params)->rowCount();
    }

    public function prepareInsert(string $table, array $columns): Statement
    {
        $values = array_values($columns);

        if ($values === $columns) {
            $values = array_fill(0, count($columns), '?');
        } else {
            $columns = array_keys($columns);

            foreach ($values as $key => $value) {
                if ($value instanceof Expr) {
                    continue;
                }
                $values[$key] = '?';
            }
        }

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES (%s)',
            $this->quoteIdentifier($table),
            implode(',', $this->quoteIdentifier($columns)),
            implode(',', $values)
        );

        return $this->prepare($sql);
    }

    public function prepareUpdate(string $table, array $columns, string $where = null): Statement
    {
        $only_column = (array_values($columns) === $columns);

        $set = [];
        foreach ($columns as $column => $value) {
            if ($only_column) {
                $set[] = $this->quoteIdentifier($value) . ' = ?';
            } else {
                $value = ($value instanceof Expr) ? $value : '?';
                $set[] = $this->quoteIdentifier($column) . ' = ' . $value;
            }
        }

        $sql = sprintf('UPDATE %s SET %s', $this->quoteIdentifier($table), implode(',', $set));
        if ($where) {
            $sql .= ' WHERE ' . $where;
        }

        return $this->prepare($sql);
    }

    public function prepareDelete(string $table, string $where = null): Statement
    {
        $table = $this->quoteIdentifier($table);

        $sql = sprintf('DELETE FROM %s', $table);
        if ($where) {
            $sql .= ' WHERE ' . $where;
        }

        return $this->prepare($sql);
    }

    /**
     * @deprecated
     */
    public function getColumns($table_name)
    {
        return $this->getTable($table_name)->getColumns();
    }

    /**
     * @deprecated
     */
    public function getIndexes($table_name)
    {
        return $this->getTable($table_name)->getIndexes();
    }
}
