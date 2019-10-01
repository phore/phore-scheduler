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
use Phore\Scheduler\Type\PhoreSchedulerTask;
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

        //$this->s = new PhoreScheduler(new PhoreSchedulerRedisConnector($redis));
        $this->s = new PhoreScheduler(new PhoreSchedulerRedisConnector("redis"));
        $this->s->defineCommand("test", function (array $arguments) {
             $this->lastRuns[] = ["test", $arguments];
        });
        $this->s->defineCommand("fail", function (array $arguments) {
            throw new \Error("test failed: {$arguments['msg']}");
        });
        $this->s->defineCommand("timeout", function (array $arguments) {
            usleep($arguments['timeout']*2000000);
        });
    }

    public function testRun()
    {
        $s = $this->s;

        $job = $s->createJob("test1");
        $job->addTask("test", ["param1" => "val1"]);
        $job->save();


        $this->assertEquals(true, $s->runNext());
        $this->assertEquals(1, count ($this->lastRuns));

        $this->assertEquals(false, $s->runNext());
        $this->assertEquals(1, count ($this->lastRuns));

    }

    public function testRetryOnTaskFailure()
    {
        $s = $this->s;

        $retryInterval = 1000; // in uSeconds

        $job = $s->createJob("test1");
        $job->addTask("fail", ["msg" => "task failed"], 3, $retryInterval/1000000, 10);
        $job->save();

        $this->assertEquals(true, $s->runNext()); //run job and set failed, return true
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(3, $task['retryCount']);
        $this->assertEquals("failed", $task['status']);

        $this->assertEquals(false, $s->runNext()); //detect failure, set pending, return false
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(2, $task['retryCount']);
        $this->assertEquals("retry", $task['status']);

        $this->assertEquals(true, $s->runNext()); //run job and set failed, return true
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(2, $task['retryCount']);
        $this->assertEquals("failed", $task['status']);

        $this->assertEquals(false, $s->runNext()); //detect failure, set pending, return false
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(1, $task['retryCount']);
        $this->assertEquals("retry", $task['status']);

        $this->assertEquals(true, $s->runNext()); //run job and set failed, return true
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(1, $task['retryCount']);
        $this->assertEquals("failed", $task['status']);

        $this->assertEquals(false, $s->runNext()); //detect failure, set pending, return false
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(0, $task['retryCount']);
        $this->assertEquals("retry", $task['status']);

        $this->assertEquals(true, $s->runNext()); //run job and set failed, return true
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(0, $task['retryCount']);
        $this->assertEquals("failed", $task['status']);

        $this->assertEquals(false, $s->runNext()); //run job and set failed, return true
        usleep($retryInterval);
        $task = $s->getJobInfo()[0]['tasks'][0];
        $this->assertEquals(0, $task['retryCount']);
        $this->assertEquals("failed", $task['status']);

    }

}