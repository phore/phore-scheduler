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

    }

    public function testRetryOnTaskFailure()
    {
        $s = $this->s;

        $retryInterval = 1000; // in uSeconds

        $job = $s->createJob("test1");
        $job->addTask("fail", ["msg" => "task failed"], 3, 10);
        $job->save();

        for($i=2; $i>=0; $i--) {
            $this->assertEquals(true, $s->runNext());
            usleep($retryInterval);
            $pendingTasks = $s->getConnector()->getPendingTasks($job->getJob()->jobId);
            $this->assertEquals($i, $pendingTasks[0]->nRetries);
        }
        $this->assertEquals(true, $s->runNext());
        $finishedTasks = $s->getConnector()->getFinishedTasks($job->getJob()->jobId);
        $this->assertEquals("failed", $finishedTasks[0]->status);

    }


}