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

    private $commands = [];

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
        $nTasks = 0;
        foreach ($tasks as $task) {
            $this->connector->addTask($job, $task);
            $nTasks++;
        }
        $job->nTasks;
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

    /**
     * @param $job PhoreSchedulerJob
     * @throws \Exception
     */
    private function _cancelTasksOnTimeout(PhoreSchedulerJob $job)
    {
        foreach ($this->connector->yieldRunningTasks($job->jobId) as $task) {
            $time = microtime(true);
            if($task->startTime + $task->timeout/1000000 < microtime(true)) {
                $this->_rescheduleTask($job, $task, "timeout");
            }
        }
    }

    private function _rescheduleTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task, string $errorMsg) {
        $task->endTime = microtime(true);
        $task->message .= $errorMsg;

        if($task->nRetries > 0) {
            $this->connector->moveRunningTaskToPending($job->jobId, $task->taskId);
            $task->nRetries--;
        } else {
            $this->connector->moveRunningTaskToDone($job->jobId, $task->taskId);
            $task->status = PhoreSchedulerTask::STATUS_FAILED;
            $job->status = PhoreSchedulerJob::STATUS_FAILED;
            $job->nFailedTasks++;
            $this->connector->updateJob($job);
        }
        $this->connector->updateTask($job->jobId, $task);

        if(!$job->continueOnFailure) {
            $this->connector->moveRunningJobToDone($job->jobId);
            $job->endTime = microtime(true);
            $this->connector->updateJob($job);
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
            $return = ($this->commands[$task->command])($task->arguments);
            $task->endTime = microtime(true);
            $task->return = phore_serialize($return);
            $task->status = PhoreSchedulerTask::STATUS_OK;
            $this->connector->updateTask($job->jobId, $task);
            $this->connector->moveRunningTaskToDone($job->jobId, $task->taskId);
            $job->nSuccessfulTasks++;
            $this->connector->updateJob($job);
        } catch (\Error $e) {
            $errorMsg = "Job failed with error: {$e->getMessage()}\n\n" . $e->getTraceAsString();
            $this->log->alert($errorMsg);
            $this->_rescheduleTask($job, $task, $errorMsg);
            return true;
        } catch (\Exception $ex) {
            $errorMsg = "Job failed with exception: {$ex->getMessage()}\n\n" . $ex->getTraceAsString();
            $this->log->alert($errorMsg);
            $this->_rescheduleTask($job, $task, $errorMsg);
            return true;
        }

        return true;

    }

    public function runNext() : bool
    {
        $this->log->debug("scanning for new tasks");
        
        if ( ! $this->connector->isConnected())
            $this->connector->connect();

        foreach ($this->connector->yieldPendingJobs() as $job) {
            if($job->runAtTs <= microtime(true) && $this->connector->movePendingJobToRunningQueue($job->jobId)) {
                $job->startTime = microtime(true);
                $this->connector->updateJob($job);
                break;
            }
        }

        $jobId = $this->connector->getRandomRunningJobId();
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

        $nRun = $this->connector->countRunningTasks($jobId);
        $nPending = $this->connector->countPendingTasks($jobId);
        $nFinished = $this->connector->countFinishedTasks($jobId);

        if($this->connector->countRunningTasks($jobId) > 0 || $this->connector->countPendingTasks($jobId) > 0) {
            return true;
        }

        $this->connector->moveRunningJobToDone($jobId);
        if($job->status !== PhoreSchedulerJob::STATUS_FAILED) {
            $job->status = PhoreSchedulerJob::STATUS_OK;
        }
        $job->endTime = microtime(true);
        $this->connector->updateJob($job);

        return true;
    }

    public function run()
    {
        while(true) {
            $this->log->notice("Starting in background mode.");
            try {
                $this->runNext();
                if (!$this->runNext()) {
                    sleep(1);
                }
            } catch (\Exception $e) {
                $this->log->alert("Exception running scheduler: " . $e->getMessage() . " (Restarting in 10sec)");
                sleep(10);
            }
        }
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

    public function getJobInfo(string $filterStatus=null, string $jobId=null) : array
    {
        $ret = [];

        if($jobId !== null) {
            $job = $this->connector->getJobById($jobId);
            $curJobInfo = (array)$job;
            $curJobInfo["tasks_all"] = $job->nTasks;
            $curJobInfo["tasks_pending"] = $this->connector->countPendingTasks($job->jobId);
            $curJobInfo["tasks_running"] = $this->connector->countRunningTasks($job->jobId);
            $curJobInfo["tasks_finished"] = $this->connector->countFinishedTasks($job->jobId);
            $curJobInfo["tasks_ok"] = $job->nSuccessfulTasks;
            $curJobInfo["tasks_failed"] = $job->nFailedTasks;
            foreach ($this->connector->getPendingTasks($job->jobId) as $task) {
                $task->status = "pending";
                $curJobInfo["tasks"][] = (array)$task;
            }
            foreach ($this->connector->getRunningTasks($job->jobId) as $task) {
                $task->status = "running";
                $curJobInfo["tasks"][] = (array)$task;
            }
            foreach ($this->connector->getFinishedTasks($job->jobId) as $task) {
                $curJobInfo["tasks"][] = (array)$task;
            }
            $ret[] = $curJobInfo;
        }

        $jobs = [];
        array_push($jobs, $this->connector->getPendingJobs());
        array_push($jobs, $this->connector->getRunningJobs());
        array_push($jobs, $this->connector->getFinishedJobs());

        foreach ($this->connector->getPendingJobs() as $job) {
            $job->status = "pending";
            $curJobInfo = (array)$job;
            $curJobInfo["tasks_all"] = $job->nTasks;
            $curJobInfo["tasks_pending"] = $this->connector->countPendingTasks($job->jobId);
            $curJobInfo["tasks_running"] = $this->connector->countRunningTasks($job->jobId);
            $curJobInfo["tasks_finished"] = $this->connector->countFinishedTasks($job->jobId);
            $curJobInfo["tasks_ok"] = $job->nSuccessfulTasks;
            $curJobInfo["tasks_failed"] = $job->nFailedTasks;
            foreach ($this->connector->getPendingTasks($job->jobId) as $task) {
                $task->status = "pending";
                $curJobInfo["tasks"][] = (array)$task;
            }
            $ret[] = $curJobInfo;
        }

        foreach ($this->connector->getRunningJobs() as $job) {
            $job->status = "running";
            $curJobInfo = (array)$job;
            $curJobInfo["tasks_all"] = $job->nTasks;
            $curJobInfo["tasks_pending"] = $this->connector->countPendingTasks($job->jobId);
            $curJobInfo["tasks_running"] = $this->connector->countRunningTasks($job->jobId);
            $curJobInfo["tasks_finished"] = $this->connector->countFinishedTasks($job->jobId);
            $curJobInfo["tasks_ok"] = $job->nSuccessfulTasks;
            $curJobInfo["tasks_failed"] = $job->nFailedTasks;
            foreach ($this->connector->getPendingTasks($job->jobId) as $task) {
                $curJobInfo["tasks"][] = (array)$task;
            }
            foreach ($this->connector->getRunningTasks($job->jobId) as $task) {
                $task->status = "running";
                $curJobInfo["tasks"][] = (array)$task;
            }
            foreach ($this->connector->getFinishedTasks($job->jobId) as $task) {
                $curJobInfo["tasks"][] = (array)$task;
            }
            $ret[] = $curJobInfo;
        }

        foreach ($this->connector->getFinishedJobs() as $job) {
            $curJobInfo = (array)$job;
            $curJobInfo["tasks_all"] = $job->nTasks;
            $curJobInfo["tasks_pending"] = $this->connector->countPendingTasks($job->jobId);
            $curJobInfo["tasks_running"] = $this->connector->countRunningTasks($job->jobId);
            $curJobInfo["tasks_finished"] = $this->connector->countFinishedTasks($job->jobId);
            $curJobInfo["tasks_ok"] = $job->nSuccessfulTasks;
            $curJobInfo["tasks_failed"] = $job->nFailedTasks;
            foreach ($this->connector->getPendingTasks($job->jobId) as $task) {
                $curJobInfo["tasks"][] = (array)$task;
            }
            foreach ($this->connector->getRunningTasks($job->jobId) as $task) {
                $curJobInfo["tasks"][] = (array)$task;
            }
            foreach ($this->connector->getFinishedTasks($job->jobId) as $task) {
                $curJobInfo["tasks"][] = (array)$task;
            }
            $ret[] = $curJobInfo;
        }
        return $ret;
    }

}
