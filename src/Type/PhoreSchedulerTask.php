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

    const STATUS_FAILED = "failed";
    const STATUS_OK = "ok";

    public $taskId;

    public $command;

    public $arguments;

    public $status;

    public $nRetries;

    public $timeout;

    public $message;

    public $return;

    public $startTime = 0.0;

    public $endTime;

    public $execHost;

    public $execPid;

    public function __construct($command, $arguments = [], $retries = 2, $timeout = 1800)
    {
        $this->taskId = uniqid();
        $this->command = $command;
        $this->arguments = $arguments;
        $this->nRetries = $retries;
        $this->timeout = $timeout * 1000000;
    }

}