<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 20.09.19
 * Time: 12:35
 */

namespace Phore\Scheduler\Type;

/**
 * Class PhoreSchedulerTask
 * @package Phore\MicroApp\Type
 * @internal
 */
class PhoreSchedulerTask
{

    const PENDING = "pending";
    const TEMPFAIL = "tempfail";
    const FAILED = "failed";
    const RUNNING = "running";
    const OK = "ok";


    public $taskId;

    public $command;

    public $arguments;

    public $status;

    public $retryCount = 3;

    public $message;

    public $return;

    public $startTime;

    public $endTime;

    public function __construct()
    {
        $this->taskId = uniqid();
    }

}