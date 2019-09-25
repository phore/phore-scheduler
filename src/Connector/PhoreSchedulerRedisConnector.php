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

    private $prefix;

    public function __construct(\Redis $redis, $prefix="PhoreScheduler")
    {
        $this->redis = $redis;
        $this->prefix = $prefix;
    }


    public function addJob(PhoreSchedulerJob $job)
    {
        $this->redis->set($this->prefix . "_job_" . $job->jobId, phore_serialize($job));
        $this->redis->lPush($this->prefix . "_jobs", $job->jobId);
    }


    public function updateJob(PhoreSchedulerJob $job)
    {
        $this->redis->set($this->prefix . "_job_" . $job->jobId, phore_serialize($job));
    }

    /**
     *
     * @return PhoreSchedulerJob[]
     */
    public function listJobs () : array
    {
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
        $this->redis->set($this->prefix . "_job_{$job->jobId}_task_{$task->taskId}", phore_serialize($task));
        $this->redis->lPush($this->prefix . "_job_{$job->jobId}_tasks", $task->taskId);
    }


    public function updateTask(PhoreSchedulerJob $job, PhoreSchedulerTask $task)
    {
        $this->redis->set($this->prefix . "_job_{$job->jobId}_task_{$task->taskId}", phore_serialize($task));
    }


    /**
     * @param PhoreSchedulerJob $job
     * @param null $filter
     * @return PhoreSchedulerTask[]
     */
    public function listTasks(PhoreSchedulerJob $job, $filter=null)
    {
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
        return $this->redis->sAdd($this->prefix . "_locked_tasks", $task->taskId);
    }


    public function unlockTask(PhoreSchedulerTask $task)
    {
        $this->redis->sRem($this->prefix . "_locked_tasks", $task->taskId);
    }
}