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

    public function __construct(string $redis_host, $prefix="PhoreScheduler")
    {
        
        $this->redis = new \Redis();
        $this->redisHost = $redis_host;            
        $this->prefix = $prefix;
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
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->set($this->prefix . "_job_" . $job->jobId, phore_serialize($job));
        $this->redis->lPush($this->prefix . "_jobs", $job->jobId);
    }


    public function updateJob(PhoreSchedulerJob $job)
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->set($this->prefix . "_job_" . $job->jobId, phore_serialize($job));
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
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->set($this->prefix . "_job_{$job->jobId}_task_{$task->taskId}", phore_serialize($task));
        $this->redis->lPush($this->prefix . "_job_{$job->jobId}_tasks", $task->taskId);
    }


    public function updateTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task)
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->set($this->prefix . "_job_{$job->jobId}_task_{$task->taskId}", phore_serialize($task));
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


    public function unlockTask(PhoreSchedulerTask $task)
    {
        if ( ! $this->connectWasCalled)
            $this->connect();
        $this->redis->sRem($this->prefix . "_locked_tasks", $task->taskId);
    }
}
