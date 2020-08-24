<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 24.09.19
 * Time: 18:47
 */

namespace Test;


use Phore\Scheduler\Connector\PhoreSchedulerRedisConnector;
use Phore\Scheduler\Type\PhoreSchedulerJob;
use Phore\Scheduler\Type\PhoreSchedulerTask;
use PHPUnit\Framework\TestCase;

class RedisConnectorTest extends TestCase
{


    /**
     * @var PhoreSchedulerRedisConnector
     */
    private $c;

    public function setUp(): void
    {
        $redis = new \Redis();
        $redis->connect("redis");
        $redis->flushAll();

        $this->c = new PhoreSchedulerRedisConnector("redis");
    }

    public function testJobIsCreated()
    {
        $c = $this->c;
        $c->addJob($j = new PhoreSchedulerJob());
        $c->addTask($j, new PhoreSchedulerTask(""));

        $this->assertEquals(1, count($jobs = $c->getPendingJobs()));
        $this->assertEquals(1, count($c->getPendingTasks($j->jobId)));
    }

    public function testGetJobById()
    {
        $job = new PhoreSchedulerJob();
        $this->c->addJob($job);
        $result = $this->c->getJobById($job->jobId);
        $this->assertEquals($job, $result);
    }

    public function testMoveRunningJobToDone()
    {
        $job = new PhoreSchedulerJob();
        $this->c->addJob($job);
        $this->c->movePendingJobToRunningQueue($job->jobId);
        $result = $this->c->getRandomRunningJobId();
        $this->assertEquals($job->jobId, $result);
        $this->c->moveRunningJobToDone($job->jobId);
        $result = $this->c->getFinishedJobs();
        $this->assertEquals($job, $result[0]);
    }

    public function testGetAndRemoveFirstPendingTask()
    {
        $job = new PhoreSchedulerJob();
        $task1 = new PhoreSchedulerTask("test1");
        $task2 = new PhoreSchedulerTask("test2");
        $this->c->addTask($job, $task1);
        $this->c->addTask($job, $task2);
        $this->c->addTask($job, new PhoreSchedulerTask("test3"));

        $firstTaskId = $this->c->getFirstPendingTaskId($job->jobId);
        $this->assertEquals($task1->taskId, $firstTaskId);

        $this->c->removeTaskFromPending($job->jobId, $firstTaskId);
        $firstTaskId = $this->c->getFirstPendingTaskId($job->jobId);
        $this->assertEquals($task2->taskId, $firstTaskId);

    }

    public function testTaskPipeline()
    {
        $job = new PhoreSchedulerJob();
        $task = new PhoreSchedulerTask("test1");
        $this->c->addTask($job, $task);

        $this->assertEquals(1, count($this->c->getPendingTasks($job->jobId)));

        $taskId = $this->c->getFirstPendingTaskId($job->jobId);
        $this->c->addTaskToRunning($job->jobId, $taskId);

        $this->assertEquals(1, $this->c->countRunningTasks($job->jobId));

        $nRemoved = $this->c->removeTaskFromPending($job->jobId, $taskId);
        $this->assertEquals(1, $nRemoved);

        $taskId = $this->c->getFirstPendingTaskId($job->jobId);
        $this->assertFalse($taskId);

        $bool = $this->c->moveRunningTaskToDone($job->jobId, $task->taskId);
        $this->assertTrue($bool);

        $result = $this->c->getFinishedTasks($job->jobId);
        $this->assertEquals(1, count($result));
        $this->assertEquals($task, $result[0]);
    }

    public function testDeleteJobById() {
        $job1 = new PhoreSchedulerJob();
        $this->c->addJob($job1);
        $task11 = new PhoreSchedulerTask("test1", [], 1, 0);
        $this->c->addTask($job1, $task11);
        $this->c->addTaskToRunning($job1->jobId, $task11->taskId);
        $t11 = $this->c->getTaskById($job1->jobId, $task11->taskId);
        $t11->startTime = microtime(true);
        $this->c->updateTask($job1->jobId, $t11);
        $task12 = new PhoreSchedulerTask("test2");
        $this->c->addTask($job1, $task12);

        $job2 = new PhoreSchedulerJob();
        $this->c->addJob($job2);
        $task21 = new PhoreSchedulerTask("test3");
        $this->c->addTask($job2, $task21);

        //job has running timed out task
        $job1->status = "cancelled";
        $this->c->updateJob($job1);
        $this->assertTrue($this->c->deleteJobById($job1->jobId));

        $this->assertEquals(0, $this->c->countPendingTasks($job1->jobId));
        $this->assertEquals(0, $this->c->countRunningTasks($job1->jobId));
        $this->assertEquals(0, $this->c->countFinishedTasks($job1->jobId));

        //job not existent
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage("Job not found.");
        $this->c->deleteJobById($job1->jobId);


    }

}