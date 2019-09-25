<?php


namespace Test;

use Phore\Scheduler\Connector\PhoreSchedulerRedisConnector;
use Phore\Scheduler\PhoreScheduler;

$redis = new \Redis();
$redis->connect("redis");

$scheduler = PhoreScheduler::Init(new PhoreSchedulerRedisConnector($redis));

$scheduler->defineCommand("test", function ($args) {
    echo "HELLO WORLD!\n";
    new Wurst();
});


$scheduler->createJob("test")->addTask("test")->save();