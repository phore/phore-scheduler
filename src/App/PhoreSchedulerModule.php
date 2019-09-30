<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 25.09.19
 * Time: 20:09
 */

namespace Phore\Scheduler\App;


use Phore\Html\Helper\Table;
use Phore\MicroApp\App;
use Phore\MicroApp\AppModule;
use Phore\Scheduler\PhoreScheduler;
use Phore\StatusPage\PageHandler\NaviButtonWithIcon;
use Phore\StatusPage\StatusPageApp;

class PhoreSchedulerModule implements AppModule
{

    private $startRoute;
    private $diName;

    public function __construct(string $startRoute = "/", $diName="phoreScheduler")
    {
        $this->startRoute = $startRoute;
        $this->diName = $diName;
    }

    public function register(App $app)
    {
        if ( ! $app instanceof StatusPageApp)
            throw new \InvalidArgumentException("This module is only suitable for StatusPageApp");

        /**
         * Joblist => Tasklist => Task Details
         */
        $app->addPage("{$this->startRoute}/scheduler/:jobId/:taskId", function (string $jobId, string $taskId, App $app) {
            $scheduler = $app->get($this->diName);
            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} schould be from type PhoreScheduler");


            $job = $scheduler->getJobInfo(null, $jobId)[0];

            $tbl = phore_array_transform($job["tasks"], function ($key, $value) use ($taskId) {
                if ($value["taskId"] == $taskId)
                    return $value;
            });


            $task = $tbl[0];
            $runTime = "--";
            if ($task["startTime"] != null) {
                $runTime = $task["startTime"];
                if ($task["endTime"] != null)
                    $runTime = $task["endTime"]- $runTime;
                else
                    $runTime = time() - $runTime;
            }

            $e = fhtml();
            $e[] = pt()->card(
                "Task Details for Task {$taskId}",
                pt("table-striped table-hover")->basic_table(
                    [
                        "TaskId", (string)$task["taskId"]
                    ],
                    [
                        [ "Status", (string)$task["status"] ],
                        [ "Start Date",  $task["startTime"] == "" ? "-- " : (string)gmdate("Y-m-d H:i:s", $task["startTime"]) . "GMT" ],
                        [ "Job scheduled at", (string)gmdate("Y-m-d H:i:s", $job["runAtTs"]) . "GMT" ],
                        [ "End Date", $task["endTime"] == "" ? "-- " : (string)gmdate("Y-m-d H:i:s", $task["endTime"]) . "GMT" ],
                        [ "Run time[s]", $runTime ],
                        [ "Command", (string)$task["command"] ],
                        [ "Arguments", ["pre @mb-0" => (string)trim (print_r($task["arguments"], true))] ],

                        [ "Message", ["pre" => (string)$task["message"]] ]
                    ],
                    ["","",""]
                )

            );
            return $e;
        });


        /**
         * Job Info
         */

        $app->addPage("{$this->startRoute}/scheduler/:jobId", function (string $jobId, App $app) {
            $scheduler = $app->get($this->diName);
            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} schould be from type PhoreScheduler");




            $tbl = phore_array_transform($scheduler->getJobInfo(null, $jobId)[0]["tasks"], function ($key, $value) use ($jobId) {
                $runTime = "--";
                if ($value["startTime"] != null) {
                    $runTime = $value["startTime"];
                    if ($value["endTime"] != null)
                        $runTime = $value["endTime"]- $runTime;
                    else
                        $runTime = time() - $runTime;
                }


                return [

                    (string)$value["taskId"],
                    (string)$value["command"],
                    (string)$value["status"],
                    (string)$value["retryCount"],

                    $value["startTime"] == "" ? "--" : gmdate("Y-m-d H:i:s", $value["startTime"]),
                    $value["endTime"] == "" ? "--" : gmdate("Y-m-d H:i:s", $value["endTime"]),
                    (string)$runTime,
                    fhtml(["a @href=? @btn @btn-primary" => "Details"], ["{$this->startRoute}/scheduler/{$jobId}/{$value["taskId"]}"])
                ];

            });


            $e = fhtml();
            $e[] = pt()->card(
                "Scheduler Task list for job {$jobId}",
                pt("table-striped table-hover")->basic_table(
                    [
                        "TaksID", "Command", "Status", "Retries",
                        "Start Time", "End Time", "RunTime", ""
                    ],
                    $tbl,
                    ["","","","", "",  "", "", "@style=text-align:right"]
                )

            );
            return $e;
        });


        /**
         * Scheduler overview
         */


        $app->addPage("{$this->startRoute}/scheduler", function (App $app) {

            $scheduler = $app->get($this->diName);
            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} schould be from type PhoreScheduler");

            $jobInfo = $scheduler->getJobInfo();


            $tbl = phore_array_transform($jobInfo, function ($index, $ji) {

                return [
                    $index + 1,
                    $ji["jobId"],
                    $ji["name"],
                    $ji["status"],

                    $ji["tasks_all"] . " Tasks (Pending: {$ji["tasks_pending"]}, Running: {$ji["tasks_running"]}, Success: {$ji["tasks_ok"]}, Failed: {$ji["tasks_failed"]})",
                    gmdate("Y-m-d H:i:s", $ji["runAtTs"]) . "GMT",
                    [
                        fhtml(["a @href=? @btn @btn-primary" => "View"], ["{$this->startRoute}scheduler/{$ji["jobId"]}"])
                    ]
                ];
            });


            $e = fhtml();
            $e[] = pt()->card(
                "Scheduler",
                pt("table-striped table-hover")->basic_table(
                    ["#", "JobID", "Job Name", "Status", "Task Status", "Scheduled at", ""],
                    $tbl,
                    ["","","", "", "", "", "@style=text-align:right"]
                )

            );

            return $e;


        },  new NaviButtonWithIcon("Scheduler", "fas fa-clock"));
    }

}
