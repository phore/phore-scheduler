<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 30.09.19
 * Time: 18:04
 */

namespace Phore\Scheduler;


use Phore\MicroApp\Type\QueryParams;
use Phore\MicroApp\Type\Request;
use Phore\Scheduler\App\PhoreSchedulerModule;
use Phore\Scheduler\Connector\PhoreSchedulerRedisConnector;
use Phore\StatusPage\StatusPageApp;

require __DIR__ . "/../vendor/autoload.php";

$app = new StatusPageApp("PhoreScheduler", "/admin");

$app->define("phoreScheduler", function() {
    $connector = new PhoreSchedulerRedisConnector("redis");
    $connector->connect();

    return new PhoreScheduler($connector);
});

$app->addModule(new PhoreSchedulerModule(""));


$app->addPage("/", function (PhoreScheduler $phoreScheduler, Request $request) {

    $phoreScheduler->defineCommand("testRunFail", function(array $args) {
        throw new \Error("test failed");
    });
    $phoreScheduler->defineCommand("testRunSuccess", function(array $args) {
        return "test successful";
    });
    $msg = "";
    if ($request->GET->has("create")) {
        $testJobRun = $phoreScheduler->createJob("test Run");
        $testJobRun->addTask("testRunFail", ["arg1"=>"argval1"], 1, 10);
        $testJobRun->addTask("testRunSuccess", ["arg1"=>"argval1"], 1, 10);
        $testJobRun->save();
        $testJobQueue = $phoreScheduler->createJob("test Queue");
        $testJobQueue->getJob()->runAtTs = time() + 60;
        $testJobQueue->addTask("testRunSuccess", ["arg1"=>"argval1"], 1, 10);
        $testJobQueue->save();
        $msg = "Job Created";

        $phoreScheduler->runNext();
    }

    if($request->GET->has("run")) {
        $phoreScheduler->runNext();
    }

    if($request->GET->has("clear")) {
        $phoreScheduler->cleanUp(0, true);
    }


    return ["h1" => "Phore Demo Scheduler Module", "a @href=?create" => "Create demo job", "p" => $msg];
});


$app->serve();

