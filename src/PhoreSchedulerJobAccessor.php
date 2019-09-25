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

    private $tasks = [];

    public function __construct(PhoreScheduler $scheduler, PhoreSchedulerJob $job)
    {
        $this->scheduler = $scheduler;
        $this->job = $job;
    }


    public function addTask(string $command, array $arguments = []) : self
    {
        $this->tasks[] = $task = new PhoreSchedulerTask();
        $task->command = $command;
        $task->arguments = $arguments;
        return $this;
    }

    public function save()
    {
        $this->scheduler->_createJob($this->job, $this->tasks);
    }


}