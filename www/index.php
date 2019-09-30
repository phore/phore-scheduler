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

$app = new StatusPageApp("TaDiS", "/admin");

$app->define("phoreScheduler", function() {
    $connector = new PhoreSchedulerRedisConnector("redis");
    $connector->connect();

    return new PhoreScheduler($connector);
});

$app->addModule(new PhoreSchedulerModule(""));


$app->addPage("/", function (PhoreScheduler $phoreScheduler, Request $request) {

    $msg = "";
    if ($request->GET->has("create")) {
        $phoreScheduler->createJob("test 01")->addTask("someTask", ["arg1"=>"argval1"])->save();
        $msg = "Job Created";
    }


    return ["h1" => "Phore Demo Scheduler Module", "a @href=?create" => "Create demo job", "p" => $msg];
});


$app->serve();

