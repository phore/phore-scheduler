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

    //protected $commands = [];

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

    public function _createJob(PhoreSchedulerJob $job, array $tasks)
    {
        //$job->status = PhoreSchedulerJob::STATUS_PENDING;

        foreach ($tasks as $task) {
            //$task->status = PhoreSchedulerJob::STATUS_PENDING;
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






    public function defineCommand(string $name, callable $fn) : self
    {
        $this->commands[$name] = $fn;
        return $this;
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


    private function _failTask($jobId, PhoreSchedulerTask $task, $message)
    {
        $task->endTime=microtime(true);
        $task->status=PhoreSchedulerTask::STATUS_FAILED;
        $task->message = $message;

        $log['starttime'] = $task->startTime;
        $log['runtime'] = $task->endTime - $task->startTime;
        $log['endtime'] = $task->endTime;
        $log['retriesLeft'] = $task->nRetries;
        $log['errorMsg'] = $message.", ".$task->message;
        $log['execHost'] = $task->execHost;
        $log['execPid'] = $task->execPid;

        $this->connector->updateTask($jobId, $task);
    }


    private function _doneTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task, $message)
    {
        $task->endTime = microtime(true);
        $task->message = $message;
        $task->status = PhoreSchedulerTask::OK;
        $this->connector->updateTask($job, $task);
        $this->connector->unlockTask($task);
    }

    private function _forceJobFailure($jobId)
    {
        $this->connector->moveRunningJobToDone($jobId);
        $job = $this->connector->getJobById($jobId);
        $job->status=PhoreSchedulerJob::STATUS_FAILED;
        $job->endTime=microtime(true);
        $this->connector->updateJob($job);

    }

    /**
     * @param $job PhoreSchedulerJob
     * @throws \Exception
     */
    private function _cancelTasksOnTimeout(PhoreSchedulerJob $job)
    {
        foreach ($this->connector->yieldRunningTasks($job->jobId) as $task) {
            if($task->startTime + $task->timeout > microtime(true)) {
                $this->_rescheduleTask($job, $task, "timeout");
            }
        }
    }

    private function _rescheduleTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task, string $errorMsg) {
        $task->endTime = microtime(true);
        $log['starttime'] = $task->startTime;
        $log['runtime'] = $task->endTime - $task->startTime;
        $log['endtime'] = $task->endTime;
        $log['retriesLeft'] = $task->nRetries;
        $log['errorMsg'] = $errorMsg;
        $log['execHost'] = $task->execHost;
        $log['execPid'] = $task->execPid;

        if($task->nRetries > 0) {
            $this->connector->moveRunningTaskToPending($job->jobId, $task->taskId);
            $task->nRetries--;
            $log['status'] = "retry";
        } else {
            $this->connector->moveRunningTaskToDone($job->jobId, $task->taskId);
            $task->status = PhoreSchedulerTask::STATUS_FAILED;
            $log['status'] = $task->status;
        }
        $this->connector->updateTask($job->jobId, $task);
        $this->connector->addTaskLog($job->jobId, $task->taskId, $log);

        if(!$job->continueOnFailure) {
            $this->_forceJobFailure($job->jobId);
            return;
        }
    }

    private function _runNextTask(PhoreSchedulerJob $job)
    {
        $task = $this->connector->getFirstPendingTask($job->jobId);
        if($task === null) {
            return;
        }
        if(!$this->connector->addTaskToRunning($job->jobId, $task->taskId))
            return;

        $task->execHost = gethostname();
        $task->execPid = getmypid();
        $task->startTime = microtime(true);
        $this->connector->updateTask($job->jobId, $task);

        if(!$this->connector->removeTaskFromPending($job->jobId, $task->taskId))
            throw new \Exception("Failed to remove pending task after copying to run.");

        try {
            $return = ($task->command)($task->arguments);
            $task->endTime = microtime(true);
            $task->return = phore_serialize($return);
            $task->status = PhoreSchedulerTask::STATUS_OK;
            $this->connector->updateTask($job->jobId, $task);

            $log['status'] = $task->status;
            $log['starttime'] = $task->startTime;
            $log['runtime'] = $task->endTime - $task->startTime;
            $log['endtime'] = $task->endTime;
            $log['retriesLeft'] = $task->nRetries;
            $log['message'] = $task->message;
            $log['return'] = $task->return;
            $log['execHost'] = $task->execHost;
            $log['execPid'] = $task->execPid;
            $this->connector->addTaskLog($job->jobId, $task->taskId, $log);
        } catch (\Error $e) {
            $errorMsg = "Job failed with error: {$e->getMessage()}\n\n" . $e->getTraceAsString();
            $this->log->alert($errorMsg);
            $this->_rescheduleTask($job, $task, $errorMsg);
            return false;
        } catch (\Exception $ex) {
            $errorMsg = "Job failed with exception: {$ex->getMessage()}\n\n" . $ex->getTraceAsString();
            $this->log->alert($errorMsg);
            $this->_rescheduleTask($job, $task, $errorMsg);
            return false;
        }

        return true;

    }

    public function runNext() : bool
    {
        $this->log->debug("scanning for new tasks");
        
        if ( ! $this->connector->isConnected())
            $this->connector->connect();

        //move pending job to queue
        $this->connector->moveRandomPendingJobToRunningQueue();
        //get random running job or return when no jobs available
        $jobId = $this->connector->getRandomRunningJob();
        if($jobId === false) {
            return false;
        }
        $job = $this->connector->getJobById($jobId);

        $this->_cancelTasksOnTimeout($job);

        if($this->connector->countRunningTasks($jobId) >= $job->nParallelTasks) {
            return true;
        }

        if(!$this->_runNextTask($job)) {
            return true;
        }

        if($this->connector->countRunningTasks($jobId) > 0 && $this->connector->countPendingTasks() > 0) {
            return true;
        }

        $this->connector->moveRunningJobToDone($jobId);

        return true;
    }

    public function run()
    {
        while(true) {
            $this->log->notice("Starting in background mode.");
            try {
                $this->runNext();
//                if (!$this->runNext()) {
//                    sleep(1);
//                }
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
