<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 25.09.19
 * Time: 14:19
 */

namespace Test;


use Phore\Scheduler\Connector\PhoreSchedulerRedisConnector;
use Phore\Scheduler\PhoreScheduler;
use PHPUnit\Framework\TestCase;

class PhoreSchedulerTest extends TestCase
{

    /**
     * @var PhoreScheduler
     */
    private $s;

    private $lastRuns = [];


    public function setUp(): void
    {
        $redis = new \Redis();
        $redis->connect("redis");
        $redis->flushAll();

        $this->lastRuns = [];

        $this->s = new PhoreScheduler(new PhoreSchedulerRedisConnector("redis"));
        $this->s->defineCommand("108", function (array $arguments) {
            return 108;
        });
        $this->s->defineCommand("test", function (array $arguments) {
             $this->lastRuns[] = ["test", $arguments];
        });
        $this->s->defineCommand("fail", function (array $arguments) {
            throw new \Error("test failed: {$arguments['msg']}");
        });
        $this->s->defineCommand("timeout", function (array $arguments) {
            usleep($arguments['timeout']);
        });
    }

    public function testRunSuccessful()
    {
        $s = $this->s;

        $job = $s->createJob("test1");
        $job->addTask("108", ["param1" => "val1"]);
        $job->save();
        $jobId = $job->getJob()->jobId;
        $taskId = $job->getTasks()[0]->taskId;
        $this->assertEquals(true, $s->runNext());
        $finTasks = $s->getConnector()->getFinishedTasks($jobId);
        $this->assertEquals($taskId, $finTasks[0]->taskId);
        $this->assertEquals("108", $finTasks[0]->return);

    }

    public function testTaskFailure()
    {
        $s = $this->s;
        $job = $s->createJob("test1");
        $job->addTask("fail", ["msg" => "task failed"], 0, 10);
        $job->save();
        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(false, $s->runNext());
    }

    public function testRetryOnTaskFailure()
    {
        $s = $this->s;
        $job = $s->createJob("test1");
        $job->addTask("fail", ["msg" => "task failed"], 3, 10);
        $job->save();

        for($i=2; $i>=0; $i--) {
            $this->assertEquals(true, $s->runNext());
            $pendingTasks = $s->getConnector()->getPendingTasks($job->getJob()->jobId);
            $this->assertEquals($i, $pendingTasks[0]->nRetries);
        }
        $this->assertEquals(true, $s->runNext());
        $finishedTasks = $s->getConnector()->getFinishedTasks($job->getJob()->jobId);
        $this->assertEquals("failed", $finishedTasks[0]->status);

    }

    public function testRunMultipleJobs() {
        //setup
        $s = $this->s;
        $job1 = $s->createJob("job1");
        $job1->addTask("test", ["task" => "1"], 3, 10);
        $task1 = $job1->getTasks()[0];
        $job1->addTask("test", ["task" => "2"], 3, 10);
        $job1->save();
        $job2 = $s->createJob("job2");
        $job2->addTask("test", ["task" => "3"], 3, 10);
        $task3 = $job2->getTasks()[0];
        $job2->addTask("fail", ["task" => "4", "msg" => "fail"], 0, 10);
        $job2->save();
        //first iteration
        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(1, $s->getConnector()->countPendingJobs());
        $this->assertEquals(1, $s->getConnector()->countRunningJobs());
        $this->assertEquals(0, $s->getConnector()->countFinishedJobs());
        $runJobId = $s->getConnector()->getRandomRunningJobId();
        $finTasks = $s->getConnector()->getFinishedTasks($runJobId);
        $this->assertArrayHasKey($finTasks[0]->taskId, [$task1->taskId => "", $task3->taskId => ""]);
        //second iteration
        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(0, $s->getConnector()->countPendingJobs());
        $this->assertEquals(2, $s->getConnector()->countRunningJobs()+$s->getConnector()->countFinishedJobs());
        //third iteration : unknown state
        $this->assertEquals(true, $s->runNext());
        //fourth iteration
        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(0, $s->getConnector()->countRunningJobs());
        $this->assertEquals(2, $s->getConnector()->countFinishedJobs());
        $this->assertEquals("failed", $s->getConnector()->getJobById($job2->getJob()->jobId)->status);
        //fifth iteration
        $this->assertEquals(false, $s->runNext());
    }

    public function testRunSequentialJobWithTimeout()
    {
        //setup
        $s = $this->s;
        $job1 = $s->createJob("job1");
        $jobId = $job1->getJob()->jobId;
        $job1->getJob()->nParallelTasks = 1;
        $job1->addTask("test", ["task" => "1"], 0, 0.1);
        $job1->addTask("test", ["task" => "2"], 3, 2);
        $job1->getTasks()[0]->startTime = microtime(true);
        $job1->save();
        $runningTaskId = $job1->getTasks()[0]->taskId;
        $s->getConnector()->addTaskToRunning($jobId, $runningTaskId);
        $s->getConnector()->removeTaskFromPending($jobId, $runningTaskId);

        //first iteration : wait for running task to finish
        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(1, $s->getConnector()->countRunningJobs());
        $this->assertEquals(1, $s->getConnector()->countPendingTasks($jobId));
        $this->assertEquals(1, $s->getConnector()->countRunningTasks($jobId));
        $this->assertEquals(0, $s->getConnector()->countFinishedTasks($jobId));

        //second iteration : clear timed out task and run next
        usleep(100000);
        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(0, $s->getConnector()->countRunningJobs());
        $this->assertEquals(1, $s->getConnector()->countFinishedJobs());
        $this->assertEquals(0, $s->getConnector()->countPendingTasks($jobId));
        $this->assertEquals(0, $s->getConnector()->countRunningTasks($jobId));
        $this->assertEquals(2, $s->getConnector()->countFinishedTasks($jobId));

        //third iteration
        $this->assertEquals(false, $s->runNext());

    }

}