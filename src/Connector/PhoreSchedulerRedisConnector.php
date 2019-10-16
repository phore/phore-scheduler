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
        return $this->redis->set($job->jobId, phore_serialize($job));
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

    public function countPendingJobs() {
        $this->ensureConnectionCalled();
        return $this->redis->sCard(self::JOBS_PENDING);
    }

    public function countRunningJobs() {
        $this->ensureConnectionCalled();
        return $this->redis->sCard(self::JOBS_RUNNING);
    }

    public function countFinishedJobs() {
        $this->ensureConnectionCalled();
        return $this->redis->sCard(self::JOBS_DONE);
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

    public function movePendingJobToDone($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->sMove(self::JOBS_PENDING, self::JOBS_DONE, $jobId);
    }

    public function getRandomRunningJobId()
    {
        $this->ensureConnectionCalled();
        return $this->redis->sRandMember(self::JOBS_RUNNING);
    }

    public function getFinishedJobs()
    {
        $this->ensureConnectionCalled();
        return $this->getJobList(self::JOBS_DONE);
    }

    /**
     * @return PhoreSchedulerJob[]
     */
    public function getPendingJobs()
    {
        $this->ensureConnectionCalled();
        return $this->getJobList(self::JOBS_PENDING);
    }

    /**
     * @return PhoreSchedulerJob[]
     */
    public function getRunningJobs()
    {
        $this->ensureConnectionCalled();
        return $this->getJobList(self::JOBS_RUNNING);
    }

    /**
     * @param string $key
     * @return PhoreSchedulerJob[]
     */
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
        return $this->redis->lLen($jobId . self::TASKS_PENDING);
    }

    public function countRunningTasks($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->sCard($jobId . self::TASKS_RUNNING);
    }

    public function countFinishedTasks($jobId)
    {
        $this->ensureConnectionCalled();
        return $this->redis->sCard($jobId . self::TASKS_DONE);
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

    public function addTaskToRunning(string $jobId, string $taskId)
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

    /**
     * @param $jobId
     * @return PhoreSchedulerTask[]
     */
    public function getPendingTasks($jobId) : array
    {
        $this->ensureConnectionCalled();
        $tasks = [];
        foreach ($this->redis->lRange($jobId . self::TASKS_PENDING, 0, -1) as $taskId) {
            $task = $this->getTaskById($jobId, $taskId);
            $task->status = PhoreSchedulerTask::STATUS_PENDING;
            $tasks[] = $task;
        }
        return $tasks;
    }

    /**
     * @param $jobId
     * @return PhoreSchedulerTask[]
     */
    public function getRunningTasks($jobId)
    {
        $this->ensureConnectionCalled();
        $tasks = [];
        foreach ($this->redis->sMembers($jobId . self::TASKS_RUNNING) as $taskId) {
            $task = $this->getTaskById($jobId, $taskId);
            $task->status = PhoreSchedulerTask::STATUS_RUNNING;
            $tasks[] = $task;
        }
        return $tasks;
    }

    /**
     * @param $jobId
     * @return PhoreSchedulerTask[]
     */
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

    public function deleteJobById($jobId) {
        $job = $this->getJobById($jobId);
        if($job === null) {
//            return false;
            throw new \Exception("Job not found.");
        }
        if($job->status !== PhoreSchedulerJob::STATUS_CANCELLED) {
//            return false;
            throw new \Exception("Job has to be cancelled.");
        }
        if($this->countRunningTasks($jobId) > 0) {
            //if job has running tasks let them finish first or cancel on timeout
            $tasks = $this->getRunningTasks($jobId);
            foreach ($tasks as $task) {
                if($task->startTime + $task->timeout > microtime(true)) {
//                    return false;
                    throw new \Exception("Job has running tasks.");
                }
            }
        }
        $keys = $this->redis->keys($jobId."*");
        $this->deleteKeys($keys);
        $this->redis->sRem(self::JOBS_PENDING, $jobId);
        $this->redis->sRem(self::JOBS_RUNNING, $jobId);
        $this->redis->sRem(self::JOBS_DONE, $jobId);
        return true;
    }

    private function deleteKeys(array $keys) {

        $this->redis->del($keys);
    }
}
