<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 24.09.19
 * Time: 09:25
 */

namespace Phore\Scheduler\Connector;


use Phore\Scheduler\Type\PhoreSchedulerJob;
use Phore\Scheduler\Type\PhoreSchedulerTask;

class PhoreSchedulerRedisConnector
{

    /**
     * @var \Redis
     */
    private $redis;
    private $redisHost;

    private $prefix;

    private $connectWasCalled = false;

    const JOBS_PENDING = "jobs_pending";
    const JOBS_RUNNING = "jobs_running";
    const JOBS_DONE = "jobs_done";
    const TASKS_PENDING = "_tasks_pending";
    const TASKS_RUNNING = "_tasks_running";
    const TASKS_DONE = "_tasks_done";

    public function __construct(string $redis_host, $prefix="PhoreScheduler")
    {
        $this->redis = new \Redis();
        $this->redisHost = $redis_host;            
        //$this->prefix = $prefix;
    }


    /**
     * Reconnect with separate connection (e.g. Multithreading)
     *
     */
    public function reconnect()
    {
        $this->redis = new \Redis();
        $this->connectWasCalled = false;
        $this->connect();
    }
    
    public function connect()
    {
        $this->redis->connect($this->redisHost);
        $this->connectWasCalled = true;
    }

    private function ensureConnection()
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
    }
    
    public function isConnected() : bool
    {
        try {
            $this->redis->ping();
            return true;
        } catch (\RedisException $e) {
            return false; 
        }        
    }
    
    public function addJob(PhoreSchedulerJob $job)
    {
        $this->ensureConnection();
        $this->redis->set($job->jobId, phore_serialize($job));
        $this->redis->sAdd(self::JOBS_PENDING, $job->jobId);
    }

    public function updateJob(PhoreSchedulerJob $job)
    {
        $this->ensureConnection();
        $this->redis->set($job->jobId, phore_serialize($job));
    }

    public function getJobById($jobId) : PhoreSchedulerJob
    {
        $this->ensureConnection();
        $job = phore_unserialize($this->redis->get($jobId), [PhoreSchedulerJob::class]);
        return ($job === false) ? null : $job;
    }

    public function moveRandomPendingJobToRunningQueue()
    {
        $this->ensureConnection();
        $jobId = $this->redis->sRandMember(self::JOBS_PENDING);
        if ($jobId !== false) {
            return $this->redis->sMove(self::JOBS_PENDING, self::JOBS_RUNNING, $jobId);
        }
        return false;
    }

    public function movePendingJobToRunningQueue($jobId)
    {
        $this->ensureConnection();
        return $this->redis->sMove(self::JOBS_PENDING, self::JOBS_RUNNING, $jobId);
    }

    public function moveRunningJobToDone($jobId)
    {
        $this->ensureConnection();
        return $this->redis->sMove(self::JOBS_RUNNING, self::JOBS_DONE, $jobId);
    }

    public function getRandomRunningJob()
    {
        $this->ensureConnection();
        return $this->redis->sRandMember(self::JOBS_RUNNING);
    }

    public function getFinishedJobs()
    {
        $this->ensureConnection();
        return $this->getJobList(self::JOBS_DONE);
    }

    public function getPendingJobs()
    {
        $this->ensureConnection();
        return $this->getJobList(self::JOBS_PENDING);
    }

    public function getRunnningJobs()
    {
        $this->ensureConnection();
        return $this->getJobList(self::JOBS_RUNNING);
    }

    private function getJobList(string $key)
    {
        $jobs = [];
        foreach ($this->redis->sMembers($key) as $jobId) {
            $jobs[] = $this->getJobById($jobId);
        }
        return $jobs;
    }



    /**
     *
     * @return PhoreSchedulerJob[]
     */
    public function listJobs () : array
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $jobs = [];
        foreach ($this->redis->lRange($this->prefix . "_jobs", 0, -1) as $jobId) {
            $jobData = $this->redis->get($this->prefix . "_job_" . $jobId);
            if ($jobData === false) {
                // Remove Missing Jobs from List
                $this->redis->lRem($this->prefix . "_jobs", $jobId, 1);
                continue;
            }
            $jobs[] = phore_unserialize($jobData, [PhoreSchedulerJob::class]);
        }
        return $jobs;
    }


    public function addTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task)
    {
        $this->ensureConnection();
        $this->redis->set($job->jobId ."_". $task->taskId, phore_serialize($task));
        $this->redis->lPush($job->jobId . self::TASKS_PENDING, $task->taskId);
    }


    public function updateTask($jobId, PhoreSchedulerTask $task)
    {
        $this->ensureConnection();
        $this->redis->set($jobId . "_" . $task->taskId, phore_serialize($task));
    }

    public function getTaskById($jobId, $taskId) : PhoreSchedulerTask
    {
        $this->ensureConnection();
        $task = phore_unserialize($this->redis->get($jobId ."_". $taskId), [PhoreSchedulerTask::class]);
        return ($task === false) ? null : $task;
    }

    public function countPendingTasks($jobId)
    {
        $this->ensureConnection();
        return $this->redis->lLen($jobId . self::TASKS_RUNNING);
    }

    public function countRunningTasks($jobId)
    {
        $this->ensureConnection();
        return $this->redis->sCard($jobId . self::TASKS_RUNNING);
    }

    public function getFirstPendingTaskId($jobId)
    {
        $this->ensureConnection();
        return $this->redis->lIndex($jobId . self::TASKS_PENDING, -1);
    }

    public function getFirstPendingTask($jobId) : ?PhoreSchedulerTask
    {
        $this->ensureConnection();
        $taskId = $this->redis->lIndex($jobId . self::TASKS_PENDING, -1);
        if($taskId === false)
            return null;
        return $this->getTaskById($jobId, $taskId);
    }

    public function addTaskToRunning($jobId, $taskId)
    {
        $this->ensureConnection();
        return $this->redis->sAdd($jobId . self::TASKS_RUNNING, $taskId);
    }

    public function removeTaskFromPending($jobId, $taskId)
    {
        $this->ensureConnection();
        return $this->redis->lRem($jobId . self::TASKS_PENDING, $taskId, 0);
    }

    public function moveRunningTaskToDone($jobId, $taskId) : bool
    {
        $this->ensureConnection();
        return $this->redis->sMove($jobId . self::TASKS_RUNNING, $jobId . self::TASKS_DONE, $taskId);
    }

    public function getPendingTasks($jobId) : array
    {
        $this->ensureConnection();
        $tasks = [];
        foreach ($this->redis->lRange($jobId . self::TASKS_PENDING, 0, -1) as $taskId) {
            $tasks[] = $this->getTaskById($jobId, $taskId);
        }
        return $tasks;
    }

    public function getFinishedTasks($jobId) : array
    {
        $this->ensureConnection();
        $tasks = [];
        foreach ($this->redis->sMembers($jobId . self::TASKS_DONE) as $taskId) {
            $tasks[] = $this->getTaskById($jobId, $taskId);
        }
        return $tasks;
    }

    /**
     * @param $jobId
     * @return \Generator|PhoreSchedulerTask[]
     */
    public function yieldRunningTasks($jobId)
    {
        $this->ensureConnection();
        foreach ($this->redis->sMembers($jobId . self::TASKS_RUNNING) as $taskId) {
            yield $this->getTaskById($jobId, $taskId);
        }
    }

    public function moveRunningTaskToPending($jobId, $taskId)
    {
        if($this->redis->rPush($jobId . self::TASKS_PENDING, $taskId) === false) {
            return false;
        }
        if($this->redis->sRem($jobId . self::TASKS_RUNNING, $taskId) !== 1) {
            throw new \Exception("failed to remove running task.");
        }
    }



    /**
     * @param PhoreSchedulerJob $job
     * @param null $filter
     * @return PhoreSchedulerTask[]
     */
    public function listTasks(PhoreSchedulerJob $job, $filter=null)
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $tasks = [];
        $taskIds = $this->redis->lRange($this->prefix . "_job_{$job->jobId}_tasks", 0, -1);
        foreach ($taskIds as $taskId) {
            $taskData = $this->redis->get($this->prefix . "_job_{$job->jobId}_task_{$taskId}");
            if ($taskData === false) {
                $this->redis->lRem($this->prefix . "_job_{$job->jobId}_tasks", $taskId, 1);
                continue;
            }
            $tasks[] = unserialize($taskData, [PhoreSchedulerTask::class]);
        }
        return $tasks;
    }


    public function rmTask(PhoreSchedulerJob $job, $taskId)
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->delete($this->prefix . "_job_{$job->jobId}_tasks", $taskId);
        $this->redis->lRem($this->prefix . "_job_{$job->jobId}_tasks", $taskId, 1);
    }

    /**
     *
     * Return:
     *
     * true     If lock was set successful
     * fals     Aleady locked
     *
     * @param PhoreSchedulerTask $task
     * @return bool
     */
    public function lockTask(PhoreSchedulerTask $task) : bool
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        return $this->redis->sAdd($this->prefix . "_locked_tasks", $task->taskId);
    }

    public function lockRescheduleTask(PhoreSchedulerTask $task) : bool
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        return $this->redis->sAdd($this->prefix . "_locked_rescheduled_tasks", $task->taskId);
    }

    public function unlockTask(PhoreSchedulerTask $task)
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->sRem($this->prefix . "_locked_tasks", $task->taskId);
    }

    /**
     * task log
     */

    public function addTaskLog($jobId, $taskId, array $log) {
        $this->ensureConnection();
        return $this->redis->sAdd($jobId ."_". $taskId . "_log", serialize($log));
    }
}
