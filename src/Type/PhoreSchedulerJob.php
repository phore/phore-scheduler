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
    const STATUS_PENDING = "pending";
    const STATUS_RUNNING = "running";
    const STATUS_FAILED = "failed";
    const STATUS_OK = "ok";
    const STATUS_CANCELLED = "cancelled";

    public $jobId;

    public $runAtTs;

    public $name;

    public $status;

    public $nTasks = 0;

    public $nParallelTasks = 100;

    public $nPendingTasks = 0;

    public $nRunningTasks = 0;

    public $nFailedTasks = 0;

    public $nSuccessfulTasks = 0;

    public $startTime;

    public $endTime;

    public $continueOnFailure = true;

    public function __construct()
    {
        $this->jobId = uniqid();
        $this->runAtTs = microtime(true);
        $this->status = self::STATUS_PENDING;
    }

}