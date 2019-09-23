<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 20.09.19
 * Time: 12:35
 */

namespace Phore\MicroApp\Type;

/**
 * Class PhoreSchedulerTask
 * @package Phore\MicroApp\Type
 * @internal
 */
class PhoreSchedulerTask
{


    public $taskId;

    public $command;

    public $arguments;

    public $failCount = 0;

    public $errorMsg;

    public $startTime;

    public $endTime;

}