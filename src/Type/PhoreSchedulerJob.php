<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 20.09.19
 * Time: 12:34
 */

namespace Phore\Scheduler\Type;

/**
 * Class PhoreSchedulerJob
 * @package Phore\MicroApp\Type
 * @internal
 */
class PhoreSchedulerJob
{
    const STATUS_FAILED = "failed";
    const STATUS_OK = "ok";

    public $jobId;

    public $runAtTs;

    public $name;

    public $status;

    public $nTasks;

    public $nParallelTasks = 100;

    public $nFailedTasks;

    public $nSuccessfulTasks;

    public $startTime;

    public $endTime;

    public $continueOnFailure = true;

    public function __construct()
    {
        $this->jobId = uniqid();
        $this->runAtTs = microtime(true);
    }

}