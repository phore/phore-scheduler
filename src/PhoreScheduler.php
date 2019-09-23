<?php


namespace Phore\Scheduler;


use Phore\MicroApp\Type\PhoreSchedulerTask;
use Phore\Scheduler\Connector\PhoreSchedulerConnector;

class PhoreScheduler
{

    /**
     * @var PhoreSchedulerConnector
     */
    private $connector;


    public function __construct(PhoreSchedulerConnector $connector)
    {
        $this->connector = $connector;
    }


    protected $commands = [];


    public function defineCommand(string $name, callable $fn) : self
    {
        $this->commands[$name] = $fn;
        return $this;
    }

    public function createJob($jobId, array $tasks=[])
    {

    }


    private function _getNextJob() : ?PhoreSchedulerTask
    {
        $jobs = $this->connector->getJobList();
        foreach ($jobs as $job) {
            if (count ($job->pendingTasks) == 0)
                continue;
            foreach ($job->pendingTasks as $index => $task) {

                if ($this->connector->tryLock($job->jobId . "#" . $task->taskId) === true) {
                    return $task;
                }
            }
        }
        return null; // No pending tasks
    }


    private function _failTask(PhoreSchedulerTask $task, $message)
    {

    }

    private function _doneTask(PhoreSchedulerTask $task, $message)
    {

    }

    public function runNext() : bool
    {

        $task = $this->_getNextJob();
        if ($task === null)
            return false;

        if ( ! isset($this->commands[$task->command]))
            $task->errorMsg = "Command undefined: '{$task->command}'";



    }

    public function run()
    {

    }





}