<?php
declare(strict_types=1);

namespace Owl\Service;

function get(string $id)
{
    $args = func_get_args();
    $container = \Owl\Service\Container::getInstance();

    return call_user_func_array([$container, 'get'], $args);
}
