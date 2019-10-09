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

    private $connectWasCalled = false;

    const JOBS_PENDING = "jobs_pending";
    const JOBS_RUNNING = "jobs_running";
    const JOBS_DONE = "jobs_done";
    const TASKS_PENDING = "_tasks_pending";
    const TASKS_RUNNING = "_tasks_running";
    const TASKS_DONE = "_tasks_done";

    public function __construct(string $redis_host)
    {
        $this->redis = new \Redis();
        $this->redisHost = $redis_host;
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

    private function ensureConnectionCalled()
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
        $this->ensureConnectionCalled();
        $this->redis->set($job->jobId, phore_serialize($job));
        $this->redis->sAdd(self::JOBS_PENDING, $job->jobId);
    }

    public function updateJob(PhoreSchedulerJob $job)
    {
        $this->ensureConnectionCalled();
        $this->redis->set($job->jobId, phore_serialize($job));
    }

    public function getJobById($jobId) : ?PhoreSchedulerJob
    {
        $this->ensureConnectionCalled();
        $job = phore_unserialize($this->redis->get($jobId), [PhoreSchedulerJob::class]);
        return ($job === false) ? null : $job;
    }

    /**
     * @return \Generator|PhoreSchedulerJob[]
     */
    public function yieldPendingJobs() {
        $this->ensureConnectionCalled();
        foreach ($this->redis->sMembers(self::JOBS_PENDING) as $jobId) {
            yield $this->getJobById($jobId);
        }
    }

    public function movePendingJobToRunningQueue($jobId) : bool
    {
        $this->ensureConnectionCalled();
        return $this->redis->sMove(self::JOBS_PENDING, self::JOBS_RUNNING, $jobId);
    }

    public function moveRunningJobToDone($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->sMove(self::JOBS_RUNNING, self::JOBS_DONE, $jobId);
    }

    public function getRandomRunningJob()
    {
        $this->ensureConnectionCalled();
        return $this->redis->sRandMember(self::JOBS_RUNNING);
    }

    public function getFinishedJobs()
    {
        $this->ensureConnectionCalled();
        return $this->getJobList(self::JOBS_DONE);
    }

    public function getPendingJobs()
    {
        $this->ensureConnectionCalled();
        return $this->getJobList(self::JOBS_PENDING);
    }

    public function getRunnningJobs()
    {
        $this->ensureConnectionCalled();
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


    public function addTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task)
    {
        $this->ensureConnectionCalled();
        $this->redis->set($job->jobId ."_". $task->taskId, phore_serialize($task));
        $this->redis->lPush($job->jobId . self::TASKS_PENDING, $task->taskId);
    }


    public function updateTask($jobId, PhoreSchedulerTask $task)
    {
        $this->ensureConnectionCalled();
        $taskString = phore_serialize($task);
        $log = microtime(true) . "_" .  $taskString;
        $this->redis->sAdd($jobId ."_". $task->taskId . "_log", $log);
        return  $this->redis->set($jobId . "_" . $task->taskId, $taskString);
    }

    public function getTaskById($jobId, $taskId) : PhoreSchedulerTask
    {
        $this->ensureConnectionCalled();
        $task = phore_unserialize($this->redis->get($jobId ."_". $taskId), [PhoreSchedulerTask::class]);
        return ($task === false) ? null : $task;
    }

    public function countPendingTasks($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->lLen($jobId . self::TASKS_RUNNING);
    }

    public function countRunningTasks($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->sCard($jobId . self::TASKS_RUNNING);
    }

    public function getFirstPendingTaskId($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->lIndex($jobId . self::TASKS_PENDING, -1);
    }

    public function getFirstPendingTask($jobId) : ?PhoreSchedulerTask
    {
        $this->ensureConnectionCalled();
        $taskId = $this->redis->lIndex($jobId . self::TASKS_PENDING, -1);
        if($taskId === false)
            return null;
        return $this->getTaskById($jobId, $taskId);
    }

    public function addTaskToRunning($jobId, $taskId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->sAdd($jobId . self::TASKS_RUNNING, $taskId);
    }

    public function removeTaskFromPending($jobId, $taskId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->lRem($jobId . self::TASKS_PENDING, $taskId, 0);
    }

    public function moveRunningTaskToDone($jobId, $taskId) : bool
    {
        $this->ensureConnectionCalled();
        return $this->redis->sMove($jobId . self::TASKS_RUNNING, $jobId . self::TASKS_DONE, $taskId);
    }

    public function getPendingTasks($jobId) : array
    {
        $this->ensureConnectionCalled();
        $tasks = [];
        foreach ($this->redis->lRange($jobId . self::TASKS_PENDING, 0, -1) as $taskId) {
            $tasks[] = $this->getTaskById($jobId, $taskId);
        }
        return $tasks;
    }

    public function getFinishedTasks($jobId) : array
    {
        $this->ensureConnectionCalled();
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
        $this->ensureConnectionCalled();
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
}
