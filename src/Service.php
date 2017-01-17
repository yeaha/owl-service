<?php
declare(strict_types=1);

namespace Owl;

abstract class Service
{
    protected $config = [];

    abstract public function disconnect();

    public function __construct(array $config = [])
    {
        $this->config = $config;
    }

    public function getConfig(string $key = '')
    {
        return ($key === '')
             ? $this->config
             : $this->config[$key] ?? false;
    }
}
