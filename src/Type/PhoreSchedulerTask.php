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
    const RETRY = "retry";
    const RUNNING = "running";
    const OK = "ok";


    public $taskId;

    public $command;

    public $arguments;

    public $status;

    public $retryCount;

    public $retryInterval;

    public $timeout;

    public $message;

    public $return;

    public $startTime = 0.0;

    public $endTime;

    public function __construct($retries = 3, $retryInterval = 60, $timeout = 600)
    {
        $this->taskId = uniqid();
        $this->retryCount = $retries;
        $this->retryInterval = $retryInterval;
        $this->timeout = $timeout;
    }

}