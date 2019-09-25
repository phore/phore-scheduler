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

        $this->s = new PhoreScheduler(new PhoreSchedulerRedisConnector($redis));
        $this->s->defineCommand("test", function (array $arguments) {
             $this->lastRuns[] = ["test", $arguments];
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

}