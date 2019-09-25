<?php


namespace Phore\Scheduler;


use Phore\Scheduler\Connector\PhoreSchedulerRedisConnector;
use Phore\Scheduler\Type\PhoreSchedulerJob;
use Phore\Scheduler\Type\PhoreSchedulerTask;

class PhoreScheduler
{

    /**
     * @var PhoreSchedulerRedisConnector
     */
    private $connector;


    public function __construct(PhoreSchedulerRedisConnector $connector)
    {
        $this->connector = $connector;
    }


    protected $commands = [];


    public function defineCommand(string $name, callable $fn) : self
    {
        $this->commands[$name] = $fn;
        return $this;
    }

    public function _createJob(PhoreSchedulerJob $job, array $tasks)
    {
        $job->status = PhoreSchedulerJob::STATUS_PENDING;

        foreach ($tasks as $task) {
            $task->status = PhoreSchedulerJob::STATUS_PENDING;
            $this->connector->addTask($job, $task);
        }
        $this->connector->addJob($job);
    }

    public function createJob(string $jobName) : PhoreSchedulerJobAccessor
    {
        $job = new PhoreSchedulerJob();
        $job->name = $jobName;
        return new PhoreSchedulerJobAccessor($this, $job);
    }


    private function _getNextTask() : ?array
    {
        $jobs = $this->connector->listJobs();
        foreach ($jobs as $job) {
            if ($job->status === PhoreSchedulerJob::STATUS_PENDING) {
                if ($job->runAtTs < time()) {
                    $job->status = PhoreSchedulerJob::STATUS_RUNNING;
                    $this->connector->updateJob($job);
                }
            }
            if ($job->status !== PhoreSchedulerJob::STATUS_RUNNING)
                continue;
            $numPending = 0;
            foreach ($this->connector->listTasks($job) as $task) {
                if (in_array($task->status, [PhoreSchedulerTask::PENDING, PhoreSchedulerTask::RUNNING]))
                    $numPending++;
                if ($task->status === PhoreSchedulerTask::PENDING) {
                    if ($this->connector->lockTask($task)) {
                        $task->status = PhoreSchedulerTask::RUNNING;
                        $task->startTime = time();
                        $this->connector->updateTask($job, $task);
                        return [$job, $task];
                    }
                }
            }
            if ($numPending === 0) {
                $job->status = PhoreSchedulerJob::STATUS_OK;
                $this->connector->updateJob($job);
            }
        }
        return null; // No pending tasks
    }


    private function _failTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task, $message)
    {
            $task->message = $message;
            $task->status = PhoreSchedulerTask::FAILED;
            $this->connector->updateTask($job, $task);
            $this->connector->unlockTask($task);
    }


    private function _doneTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task, $message)
    {
            $task->message = $message;
            $task->status = PhoreSchedulerTask::OK;
            $this->connector->updateTask($job, $task);
            $this->connector->unlockTask($task);
    }

    public function runNext() : bool
    {

        $nextTask = $this->_getNextTask();
        if ($nextTask === null) {

            return false;
        }
        [$job, $task] = $nextTask;
        /* @var $job PhoreSchedulerJob */
        /* @var $task PhoreSchedulerTask */

        if ( ! isset($this->commands[$task->command])) {
            $this->_failTask($job, $task, "command undefine: '{$task->command}'");
        }

        try {
            $task->startTime = microtime(true);
            $this->connector->updateTask($job, $task);
            $return = ($this->commands[$task->command])($task->arguments);
            $task->endTime = microtime(true);
            $task->return = phore_serialize($return);
            $this->_doneTask($job, $task, "OK");
        } catch (\ErrorException $e) {
            $this->_failTask($job, $task, "Error: " . $e->getMessage() . "\n\n" . $e->getTraceAsString());
        } catch (\Exception $ex) {
            $this->_failTask($job, $task, "Exception: " . $ex->getMessage() . "\n\n" . $ex->getTraceAsString());
        }
        return true;
    }

    public function run()
    {
        while(true) {
            if ( ! $this->runNext()) {
                sleep(1);
            }
        }
    }


    public function getJobInfo(string $filterStatus=null) : array
    {
        $ret = [];

        foreach ($this->connector->listJobs() as $job) {
            if ($filterStatus !== null && $job->status !== $filterStatus) {
                continue;
            }
            $curJobInfo = (array)$job;
            $tasks = $this->connector->listTasks($job);
            $curJobInfo["tasks"] = (array)$tasks;

            $curJobInfo["tasks"] = 0;
            $curJobInfo["tasks_pending"] = 0;
            $curJobInfo["tasks_running"] = 0;
            $curJobInfo["tasks_ok"] = 0;
            $curJobInfo["tasks_failed"] = 0;

            foreach ($tasks as $task) {
                $curJobInfo["tasks"]++;
                switch ($task->status) {
                    case $task::RUNNING:
                        $curJobInfo["tasks_running"]++;
                        break;
                    case $task::PENDING:
                        $curJobInfo["tasks_pending"]++;
                        break;
                    case $task::OK:
                        $curJobInfo["tasks_ok"]++;
                        break;
                    case $task::FAILED:
                        $curJobInfo["tasks_failed"]++;
                        break;
                }
            }

            $ret[] = $curJobInfo;
        }
        return $curJobInfo;
    }




}