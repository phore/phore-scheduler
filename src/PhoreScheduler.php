<?php


namespace Phore\Scheduler;


use Phore\Log\Logger\PhoreNullLogger;
use Phore\Log\PhoreLogger;
use Phore\Scheduler\Connector\PhoreSchedulerRedisConnector;
use Phore\Scheduler\Type\PhoreSchedulerJob;
use Phore\Scheduler\Type\PhoreSchedulerTask;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class PhoreScheduler implements LoggerAwareInterface
{

    /**
     * @var PhoreSchedulerRedisConnector
     */
    private $connector;

    protected $commands = [];

    /**
     * @var NullLogger
     */
    protected $log;


    public function __construct(PhoreSchedulerRedisConnector $connector)
    {
        $this->connector = $connector;
        $this->log = new NullLogger();
    }



    public function getConnector() : PhoreSchedulerRedisConnector
    {
        return $this->connector;
    }

    public function setLogger(LoggerInterface $logger) : self
    {
        $this->log = $logger;
        return $this;
    }


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
                if ($job->runAtTs <= time()) {
                    $job->status = PhoreSchedulerJob::STATUS_RUNNING;
                    $this->connector->updateJob($job);
                }
            }
            if ($job->status !== PhoreSchedulerJob::STATUS_RUNNING)
                continue;
            $numPending = 0;
            foreach ($this->connector->listTasks($job) as $task) {
                if ($task->startTime > microtime(true)) {
                    // skip retries until retryInterval reached, but note as pending
                    $numPending++;
                    continue;
                }
                if (in_array($task->status, [PhoreSchedulerTask::PENDING, PhoreSchedulerTask::RUNNING, PhoreSchedulerTask::RETRY]))
                    $numPending++;
                if ($task->status === PhoreSchedulerTask::PENDING || $task->status === PhoreSchedulerTask::RETRY) {
                    if ($this->connector->lockTask($task)) {
                        $task->status = PhoreSchedulerTask::RUNNING;
                        $task->startTime = microtime(true);
                        $this->connector->updateTask($job, $task);
                        return [$job, $task];
                    }
                }
                if ($task->status === PhoreSchedulerTask::FAILED) {
                    if($task->retryCount > 0 ) {
                        $task->retryCount--;
                        $task->status = PhoreSchedulerTask::RETRY;
                        $numPending++;
                        $task->startTime+=$task->retryInterval;
                        $this->connector->updateTask($job, $task);
                        continue;
                    }
                }
                if ($task->startTime+$task->timeout<microtime(true)) {
                    $task->status = PhoreSchedulerTask::FAILED;
                    $this->connector->updateTask($job, $task);
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
        $task->endTime = microtime(true);
        $task->message = $message;
        $task->status = PhoreSchedulerTask::FAILED;
        $this->connector->updateTask($job, $task);
        $this->connector->unlockTask($task);
    }


    private function _doneTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task, $message)
    {
        $task->endTime = microtime(true);
        $task->message = $message;
        $task->status = PhoreSchedulerTask::OK;
        $this->connector->updateTask($job, $task);
        $this->connector->unlockTask($task);
    }

    public function runNext() : bool
    {
        $this->log->debug("scanning for new tasks");
        
        if ( ! $this->connector->isConnected())
            $this->connector->connect();
        
        $nextTask = $this->_getNextTask();
        if ($nextTask === null) {
            $this->log->debug("no new tasks to be processed");
            return false;
        }
        [$job, $task] = $nextTask;
        /* @var $job PhoreSchedulerJob */
        /* @var $task PhoreSchedulerTask */

        if ( ! isset($this->commands[$task->command])) {
            $this->log->alert("Job: :jobName Task: :taskId Command :command undefined", [
                "jobName" => $job->jobName,
                "taskId" => $task->taskId,
                "command" => $task->command
            ]);
            $this->_failTask($job, $task, "command undefine: '{$task->command}'");
        }

        try {
            $this->log->notice("running task :taskId (Command: :command)", [
                "taskId" => $task->taskId,
                "command" => $task->command,
                "arguments" => phore_json_encode($task->arguments)
            ]);
            $task->startTime = microtime(true);
            $this->connector->updateTask($job, $task);
            $return = ($this->commands[$task->command])($task->arguments);
            $task->return = phore_serialize($return);
            $this->_doneTask($job, $task, "OK");
            $this->log->notice("Job successful");
        } catch (\Error $e) {
            $this->log->alert("Job failed with error: {$e->getMessage()}\n\n" . $e->getTraceAsString());
            $this->_failTask($job, $task, "Error: " . $e->getMessage() . "\n\n" . $e->getTraceAsString());
        } catch (\Exception $ex) {
            $this->log->alert("Job failed with exception: {$ex->getMessage()}\n\n" . $ex->getTraceAsString());
            $this->_failTask($job, $task, "Exception: " . $ex->getMessage() . "\n\n" . $ex->getTraceAsString());
        }
        return true;
    }

    public function run()
    {
        while(true) {
            $this->log->notice("Starting in background mode.");
            try {
                                
                if (!$this->runNext()) {
                    sleep(1);
                }
            } catch (\Exception $e) {
                $this->log->alert("Exception running scheduler: " . $e->getMessage() . " (Restarting in 10sec)");
                sleep(10);
            }
        }
    }




    public function getJobInfo(string $filterStatus=null, string $jobId=null) : array
    {
        $ret = [];

        foreach ($this->connector->listJobs() as $job) {

            if ($filterStatus !== null && $job->status !== $filterStatus) {
                continue;
            }
            if ($jobId !== null && $jobId !== $job->jobId)
                continue;

            $curJobInfo = (array)$job;
            $tasks = $this->connector->listTasks($job);
            $curJobInfo["tasks"] = [];

            $curJobInfo["tasks_all"] = 0;
            $curJobInfo["tasks_pending"] = 0;
            $curJobInfo["tasks_running"] = 0;
            $curJobInfo["tasks_ok"] = 0;
            $curJobInfo["tasks_failed"] = 0;

            foreach ($tasks as $task) {
                $curJobInfo["tasks"][] = (array)$task;
                $curJobInfo["tasks_all"]++;
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
        return $ret;
    }




    private static $instance = null;

    public static function Init(PhoreSchedulerRedisConnector $connector) : self {
        self::$instance = new self($connector);
        return self::$instance;
    }

    public static function GetSingleton() : self
    {
        if (self::$instance === null)
            throw new \InvalidArgumentException("Scheduler not initialized. Call PhoreScheduler::Init()");
        return self::$instance;
    }


}
