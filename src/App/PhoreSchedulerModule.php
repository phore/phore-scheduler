<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 25.09.19
 * Time: 20:09
 */

namespace Phore\Scheduler\App;


use Phore\MicroApp\App;
use Phore\MicroApp\AppModule;
use Phore\MicroApp\Type\Request;
use Phore\MicroApp\Type\RouteParams;
use Phore\Scheduler\PhoreScheduler;
use Phore\StatusPage\PageHandler\NaviButtonWithIcon;
use Phore\StatusPage\StatusPageApp;

class PhoreSchedulerModule implements AppModule
{

    private $startRoute;
    private $diName;

    public function __construct(string $startRoute = "", $diName="phoreScheduler")
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
                throw new \InvalidArgumentException("{$this->diName} should be of type PhoreScheduler");


            $job = $scheduler->getJobInfo(null, $jobId)[0];

            $tbl = phore_array_transform($job["tasks"], function ($key, $value) use ($taskId) {
                if ($value["taskId"] == $taskId)
                    return $value;
            });


            $task = $tbl[0];
            $runTime = "--";
            if ($task["startTime"] !== null) {
                $runTime = $task["startTime"];
                if ($task["endTime"] !== null)
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
                        [ "Host", (string)$task["execHost"] ],
                        [ "PID", (string)$task["execPid"] ],
                        [ "Start Date",  $task["startTime"] == "" ? "-- " : (string)gmdate("Y-m-d H:i:s", (int) $task["startTime"]) . "GMT" ],
                        [ "Job scheduled at", (string)gmdate("Y-m-d H:i:s", $job["runAtTs"]) . "GMT" ],
                        [ "End Date", $task["endTime"] == "" ? "-- " : (string)gmdate("Y-m-d H:i:s", (int) $task["endTime"]) . "GMT" ],
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

        $app->addPage("{$this->startRoute}/scheduler/:jobId", function (string $jobId, App $app, Request $request) {
            $scheduler = $app->get($this->diName);

            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} should be from type PhoreScheduler");

            $filterStatus = $request->GET->get("status", null);

            $tbl = phore_array_transform($scheduler->getJobInfo($filterStatus, $jobId)[0]["tasks"], function ($key, $value) use ($jobId) {
                $runTime = "--";
                if ($value["startTime"] != null) {
                    $runTime = $value["startTime"];
                    if ($value["endTime"] === null)
                        $runTime = time() - $runTime;
                    else
                        $runTime = $value["endTime"]- $runTime;
                }

                return [
                    $key+1,
                    (string)$value["taskId"],
                    (string)$value["command"],
                    (string)$value["status"],
                    (string)$value["nRetries"],

                    $value["startTime"] == "" ? "--" : gmdate("Y-m-d H:i:s", (int) $value["startTime"]),
                    $value["endTime"] == "" ? "--" : gmdate("Y-m-d H:i:s", (int) $value["endTime"]),
                    (string)$runTime,
                    fhtml(["a @href=? @btn @btn-primary" => "Details"], ["{$this->startRoute}/scheduler/{$jobId}/{$value["taskId"]}"])
                ];

            });


            $e = fhtml();
            $e[] = pt()->card(
                "Scheduler Task list for job {$jobId}",
                pt("table-striped table-hover")->basic_table(
                    [
                        "#", "TaksID", "Command", "Status", "Retries",
                        "Start Time", "End Time", "RunTime", ""
                    ],
                    $tbl,
                    ["","","","","", "",  "", "", "@style=text-align:right"]
                )

            );
            return $e;
        });


        /**
         * Scheduler overview
         */


        $app->addPage("{$this->startRoute}/scheduler", function (App $app, Request $request) {

            $scheduler = $app->get($this->diName);
            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} should be from type PhoreScheduler");

            $jobId = $request->GET->get("jobId", false);
            if($jobId !== false) {
                $mode = $request->GET->get("jobId", false);
                switch ($mode) {
                    case "cancel":
                        $scheduler->cancelJob($jobId);
                        break;
                    case "del":
                        $scheduler->deleteJob($jobId);
                        break;
                }
            }

            $jobInfo = $scheduler->getJobInfo();


            $tbl = phore_array_transform($jobInfo, function ($index, $ji) {

                return [
                    $index + 1,
                    $ji["jobId"],
                    $ji["name"],
                    $ji["status"],

                    $ji["nTasks"] . " Tasks (Pending: {$ji["tasks_pending"]}, Running: {$ji["tasks_running"]}, Success: {$ji["nSuccessfulTasks"]}, Failed: {$ji["nFailedTasks"]})",
                    gmdate("Y-m-d H:i:s", $ji["runAtTs"]) . "GMT",
                    [
                        fhtml(["a @href=? @btn @btn-primary" => "View"], ["{$this->startRoute}/scheduler/{$ji["jobId"]}"]),
                        fhtml(["a @href=? @btn @btn-danger" => "Cancel"], ["{$this->startRoute}/scheduler?mode=cancel&jobId={$ji["jobId"]}"]),
                        fhtml(["a @href=? @btn @btn-danger" => "Del"], ["{$this->startRoute}/scheduler?mode=del&jobId={$ji["jobId"]}"])
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
