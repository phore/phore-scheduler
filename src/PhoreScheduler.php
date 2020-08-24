<?php


namespace Phore\Scheduler;


use http\Exception;
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

    /**
     * @param PhoreSchedulerJob $job
     * @param PhoreSchedulerTask[] $tasks
     */
    public function _createJob(PhoreSchedulerJob $job, array $tasks)
    {
        $nTasks = 0;
        foreach ($tasks as $task) {
//            $task->status = PhoreSchedulerTask::STATUS_PENDING;
            $this->connector->addTask($job, $task);
            $nTasks++;
        }
        $job->nTasks = $nTasks;
        $job->status = PhoreSchedulerJob::STATUS_PENDING;
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

    public function cancelJob($jobId)
    {
        if(!$this->connector->moveRunningJobToDone($jobId)) {
            $this->connector->movePendingJobToDone($jobId);
        }

        $job = $this->connector->getJobById($jobId);
        if($job !== null) {
            $job->status = PhoreSchedulerJob::STATUS_CANCELLED;
            $job->endTime = microtime(true);
            $this->connector->updateJob($job);
        }

    }

    public function pauseJob($jobId)
    {
        //TODO: Pause or reschedule job to future
    }

    public function deleteJob($jobId)
    {
        // return true or error msg
        try {
            $msg = $this->connector->deleteJobById($jobId);
        } catch (\Exception $e) {
            $msg = $e;
        }
        return $msg;
    }

    public function retryJob($jobId) {
        if($this->connector->getTasksFailCount($jobId) === 0)
            return false;
        $finishedTasks = $this->connector->getFinishedTasks($jobId);
        $failedTasks = [];
        foreach ($finishedTasks as $task) {
            if($task->status === PhoreSchedulerTask::STATUS_FAILED) {
                $clone = clone $task;
                $clone->startTime = 0.0;
                $clone->endTime = null;
                $clone->status = null;
                $clone->taskId = uniqid();
                $clone->nRetries = 1;
                $failedTasks[] = $clone;
            }
        }
        $oldJob = $this->connector->getJobById($jobId);
        $retryJob = new PhoreSchedulerJob();
        $retryJob->name = $oldJob->name . "_retry";
        $this->_createJob($retryJob, $failedTasks);
        return true;
    }

    private function _validateFinishedJobState($jobId)
    {
        $job = $this->connector->getJobById($jobId);

        if($job->nPendingTasks > 0) {
            return false;
        }
        if($job->nRunningTasks > 0) {
            return false;
        }
        if($this->connector->getTasksSuccessCount($jobId) === $job->nTasks && $job->status !== PhoreSchedulerJob::STATUS_OK) {
            return false;
        }

        return true;
    }

    /**
     * @param $job PhoreSchedulerJob
     * @throws \Exception
     */
    private function _cancelTasksOnTimeout(PhoreSchedulerJob $job)
    {
        foreach ($this->connector->yieldRunningTasks($job->jobId) as $task) {
            if($task->startTime + $task->timeout < microtime(true)) {
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
            $job = $this->connector->getJobById($job->jobId);
            $job->status = PhoreSchedulerJob::STATUS_FAILED;
            $job->nFailedTasks = $this->connector->incrementTasksFailCount($job->jobId);;
            $this->connector->updateJob($job);
        }
        $this->connector->updateTask($job->jobId, $task);

        if(!$job->continueOnFailure) {
            $this->connector->moveRunningJobToDone($job->jobId);
            $job = $this->connector->getJobById($job->jobId);
            $job->endTime = microtime(true);
            $this->connector->updateJob($job);
        }
    }

    /**
     * @param PhoreSchedulerJob $job
     * @return bool FALSE if no pending tasks were found, else TRUE
     * @throws \Exception
     */
    private function _runNextTask(PhoreSchedulerJob $job) : bool
    {
        $task = $this->connector->getFirstPendingTask($job->jobId);
        if($task === null) {
            return false;
        }
        if(!$this->connector->addTaskToRunning($job->jobId, $task->taskId))
            return true;

        $task->execHost = gethostname();
        $task->execPid = getmypid();
        $task->startTime = microtime(true);
        $task->endTime = null; // reset in case this task has run before and failed
        $this->connector->updateTask($job->jobId, $task);

        if(!$this->connector->removeTaskFromPending($job->jobId, $task->taskId))
            throw new \Exception("Failed to remove pending task after copying to run.");

        try {
            $return = ($this->commands[$task->command])($task->arguments);
            $task->endTime = microtime(true);
            $task->return = $return;
            $task->status = PhoreSchedulerTask::STATUS_OK;
            $this->connector->updateTask($job->jobId, $task);
            $this->connector->moveRunningTaskToDone($job->jobId, $task->taskId);
            $job = $this->connector->getJobById($job->jobId);
            $job->nSuccessfulTasks = $this->connector->incrementTasksSuccessCount($job->jobId);
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
            if($this->connector->countPendingTasks($job->jobId) == 0 &&
                $this->connector->countRunningTasks($job->jobId) == 0 &&
                $this->connector->countFinishedTasks($job->jobId) == 0 &&
                $this->connector->movePendingJobToDone($job->jobId)
            ) {
                $job->status = PhoreSchedulerJob::STATUS_FAILED;
                $this->connector->updateJob($job);
                $this->log->debug("moved empty pending job '{$job->jobId}' to done (failed)");
                break;
            }
            if($job->runAtTs <= microtime(true) && $this->connector->movePendingJobToRunningQueue($job->jobId)) {
                $job->status = PhoreSchedulerJob::STATUS_RUNNING;
                $job->startTime = microtime(true);
                $this->connector->updateJob($job);
                $this->log->debug("moved pending job '{$job->jobId}' to running");
                break;
            }
        }

        $jobId = $this->connector->getRandomRunningJobId();
        if($jobId === false) {
            $this->log->debug("No running jobs");
            return false;
        } else {
            $this->log->debug("Start working on job '{$jobId}'.");
        }
        $job = $this->connector->getJobById($jobId);

        $this->_cancelTasksOnTimeout($job);

        if($this->connector->countRunningTasks($jobId) >= $job->nParallelTasks) {
            return true;
        }

        if(!$this->_runNextTask($job)) {
            //no pending tasks were found. If all tasks are finished we can try to move this Job to done
            //also make sure the job has a 'done' status (STATUS_CANCELLED, STATUS_FAILED or STATUS_OK)
            $finishedStatus = [PhoreSchedulerJob::STATUS_CANCELLED, PhoreSchedulerJob::STATUS_FAILED, PhoreSchedulerJob::STATUS_OK];
            if($this->connector->countFinishedTasks($jobId) === $job->nTasks && in_array($job->status, $finishedStatus)) {
                $this->connector->moveRunningJobToDone($jobId);
            }
            return true;
        }

        if($this->connector->countRunningTasks($jobId) > 0 || $this->connector->countPendingTasks($jobId) > 0) {
            return true;
        }

        $job = $this->connector->getJobById($jobId); // get updated version in case status changed
        if($job->status === PhoreSchedulerJob::STATUS_CANCELLED) {
            return true;
        }

        $this->connector->moveRunningJobToDone($jobId);
        if($job->status !== PhoreSchedulerJob::STATUS_FAILED) {
            $job->status = PhoreSchedulerJob::STATUS_OK;
        }
        $job->endTime = microtime(true);
        $this->connector->updateJob($job);
        if(!$this->_validateFinishedJobState($jobId)) {
            $this->log->alert("Finished job state has errors");
        }

        return true;
    }

    public function cleanUp(int $age = 3600, $onlySuccess = true)
    {
        if ( ! $this->connector->isConnected())
            $this->connector->connect();

        $finishedJobs = $this->connector->getFinishedJobs();
        $deletedJobs = 0;
        foreach ($finishedJobs as $job) {
            if($job->endTime+$age > microtime(true))
                continue;
            if($job->status === PhoreSchedulerJob::STATUS_CANCELLED) {
                if($this->deleteJob($job->jobId) === true)
                    $deletedJobs++;
                continue;
            }
            if($onlySuccess && $job->status !== PhoreSchedulerJob::STATUS_OK)
                continue;
            $this->cancelJob($job->jobId);
        }
        if($deletedJobs > 0) {
            $this->log->notice("CleanUp deleted $deletedJobs out of " . count($finishedJobs) . " finished jobs.");
        }
    }

    public function run()
    {
        $this->log->notice("Starting in background mode.");
        while(true) {
            try {
                // when no jobs are available clean up and sleep
                if($this->runNext() === false) { //sleep when no job
                    $this->log->debug("No jobs to process. Starting cleanup.");
                    $this->cleanUp();
                    usleep(200000);
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


    public function getJobOverview(string $filterStatus=null)
    {
        $return = $jobs = [];
        switch ($filterStatus) {
            case "pending":
                $jobs = $this->connector->getPendingJobs();
                break;
            case "running":
                $jobs = $this->connector->getRunningJobs();
                break;
            case "success":
            case "failed":
            case "cancelled":
            case "finished":
                $jobs = $this->connector->getFinishedJobs();
                break;
            case "all":
                $jobs = array_merge(
                    $this->connector->getPendingJobs(),
                    $this->connector->getRunningJobs(),
                    $this->connector->getFinishedJobs()
                );
                break;
            default:
                $jobs = array_merge(
                    $this->connector->getPendingJobs(),
                    $this->connector->getRunningJobs(),
                    $this->connector->getFinishedJobs([PhoreSchedulerJob::STATUS_FAILED])
                );
        }

        foreach ($jobs as $job) {
            if($filterStatus === "success") {
                if($job->status !== PhoreSchedulerJob::STATUS_OK)
                    continue;
            }
            if($filterStatus === "failed") {
                if($job->status !== PhoreSchedulerJob::STATUS_FAILED)
                    continue;
            }
            if($filterStatus === "cancelled") {
                if($job->status !== PhoreSchedulerJob::STATUS_CANCELLED)
                    continue;
            }
            if($filterStatus === "pending") {
                if($job->status !== PhoreSchedulerJob::STATUS_PENDING)
                    continue;
            }
            if($filterStatus === "running") {
                if($job->status !== PhoreSchedulerJob::STATUS_RUNNING)
                    continue;
            }
            $curJobInfo = (array)$job;
            $curJobInfo["tasks_pending"] = $this->connector->countPendingTasks($job->jobId);
            $curJobInfo["tasks_running"] = $this->connector->countRunningTasks($job->jobId);
            $curJobInfo["tasks_finished"] = $this->connector->countFinishedTasks($job->jobId);
            $curJobInfo["tasks_ok"] = $this->connector->getTasksSuccessCount($job->jobId);
            $curJobInfo["tasks_failed"] = $this->connector->getTasksFailCount($job->jobId);
            $return[] = $curJobInfo;
        }

        return $return;

    }

    public function getJobDetails(string $jobId=null, string $filterStatus=null)
    {
        $return = [];
        $job = $this->connector->getJobById($jobId);
        if($job === null) {
            return $return;
        }

        $return = (array)$job;
        $return["tasks_ok"] = $this->connector->getTasksSuccessCount($job->jobId);
        $return["tasks_failed"] = $this->connector->getTasksFailCount($job->jobId);

        switch ($filterStatus) {
            case "pending":
                $tasks = $this->connector->getPendingTasks($jobId);
                break;
            case "running":
                $tasks = $this->connector->getRunningTasks($jobId);
                break;
            case "success":
            case "failed":
            case "finished":
                $tasks = $this->connector->getFinishedTasks($jobId);
                break;
            default:
                $tasks = array_merge(
                    $this->connector->getPendingTasks($jobId),
                    $this->connector->getRunningTasks($jobId),
                    $this->connector->getFinishedTasks($jobId)
                );
        }

        $return["tasks"] = [];
        foreach ($tasks as $task) {
            if($filterStatus === "success") {
                if($task->status !== PhoreSchedulerTask::STATUS_OK)
                    continue;
            }
            if($filterStatus === "failed") {
                if($task->status !== PhoreSchedulerTask::STATUS_FAILED)
                    continue;
            }
            $return["tasks"][] = (array)$task;
        }

        return $return;
    }

    public function getTaskDetails(string $jobId, string $taskId)
    {
        $return = [];
        $job = $this->connector->getJobById($jobId);
        if($job === null) {
            return $return;
        }
        $task = $this->connector->getTaskById($jobId, $taskId);
        if($task === null) {
            return $return;
        }
        if($task->status === null) {
            $task->status = PhoreSchedulerTask::STATUS_PENDING;
            foreach($this->connector->yieldRunningTasks($jobId) as $t) {
                if($t->taskId === $taskId) {
                    $task->status = PhoreSchedulerTask::STATUS_RUNNING;
                    break;
                }
            }
        }
        $return = (array)$task;
        $return["jobStart"] = $job->startTime;
        $return["jobRunAt"] = $job->runAtTs;
        return $return;

    }
}
