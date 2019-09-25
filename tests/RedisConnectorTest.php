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

        $this->c = new PhoreSchedulerRedisConnector($redis);
    }


    public function testJobIsCreated()
    {
        $c = $this->c;

        $c->addJob($j = new PhoreSchedulerJob());
        $c->addTask($j, new PhoreSchedulerTask());

        $this->assertEquals(1, count ($jobs = $c->listJobs()));
        $this->assertEquals(1, count($c->listTasks($jobs[0])));
    }


    public function testJobLock()
    {
        $c = $this->c;

        $task = new PhoreSchedulerTask();

        $ret = $c->lockTask($task);
        $this->assertEquals(true, $ret);

        $ret = $c->lockTask($task);
        $this->assertEquals(false, $ret);

        $c->unlockTask($task);

    }


}