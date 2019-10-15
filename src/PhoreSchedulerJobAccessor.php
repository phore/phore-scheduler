<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 25.09.19
 * Time: 14:23
 */

namespace Phore\Scheduler;


use Phore\Scheduler\Type\PhoreSchedulerJob;
use Phore\Scheduler\Type\PhoreSchedulerTask;
use Phore\Scheduler\PhoreScheduler;

class PhoreSchedulerJobAccessor
{

    /**
     * @var PhoreScheduler
     */
    private $scheduler;

    /**
     * @var PhoreSchedulerJob
     */
    private $job;

    /**
     * @var PhoreSchedulerTask[]
     */
    private $tasks = [];

    public function __construct(PhoreScheduler $scheduler, PhoreSchedulerJob $job)
    {
        $this->scheduler = $scheduler;
        $this->job = $job;
    }


    public function addTask(string $command, array $arguments = [], $retries = 2, $timeout = 1800) : self
    {
        $this->tasks[] = new PhoreSchedulerTask($command, $arguments, $retries, $timeout);
        return $this;
    }

    public function save()
    {
        $this->scheduler->_createJob($this->job, $this->tasks);
    }

    /**
     * @return PhoreSchedulerJob
     */
    public function getJob(): PhoreSchedulerJob
    {
        return $this->job;
    }

    /**
     * @return PhoreSchedulerTask[]
     */
    public function getTasks(): array
    {
        return $this->tasks;
    }




}