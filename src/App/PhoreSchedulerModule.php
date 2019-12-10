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
use Phore\Scheduler\Type\PhoreSchedulerJob;
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


            $task = $scheduler->getTaskDetails($jobId, $taskId);
            if(empty($task)) {
                throw new \InvalidArgumentException("Task not found.");
            }

            $runTime = "--";
            if ($task["startTime"] != null) {
                $runTime = $task["startTime"];
                if ($task["endTime"] !== null) {
                    $runTime = $task["endTime"]- $runTime;
                } else {
                    $runTime = time() - $runTime;
                    if($runTime > $task['timeout']) {
                        $runTime = "timeout (" . (int) $runTime . ")";
                    }

                }
            }

            $jobStart = "--";
            if ($task["jobStart"] != 0) {
                $jobStart = (string)gmdate("Y-m-d H:i:s", (int) $task["jobStart"]) . "GMT";
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
                        [ "Job", fhtml(["a @href=?"  => "{$jobId}"], ["{$this->startRoute}/scheduler/{$jobId}"]) ],
                        [ "Job Start Date", $jobStart . " (scheduled at ".(string)gmdate("Y-m-d H:i:s", (int) $task["jobRunAt"]).")  " ],
                        [ "Start Date",  $task["startTime"] == "" ? "-- " : (string)gmdate("Y-m-d H:i:s", (int) $task["startTime"]) . "GMT" ],
                        [ "End Date", $task["endTime"] == "" ? "-- " : (string)gmdate("Y-m-d H:i:s", (int) $task["endTime"]) . "GMT" ],
                        [ "Run time[s]", $runTime ],
                        [ "Timeout[s]", $task["timeout"] ],
                        [ "Remaining Retries", $task["nRetries"] ],
                        [ "Command", (string)$task["command"] ],
                        [ "Arguments", ["pre @mb-0" => (string)trim (print_r($task["arguments"], true))] ],
                        [ "Return Value", ["pre @mb-0" => (string)trim (print_r($task["return"], true))] ],
                        [ "Message", ["pre" => (string)$task["message"]] ]
                    ],
                    ["","",""]
                )

            );
            return $e;
        });


        /**
         * Job Details and Task List
         */

        $app->addPage("{$this->startRoute}/scheduler/:jobId", function (string $jobId, App $app, Request $request) {
            $scheduler = $app->get($this->diName);

            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} should be from type PhoreScheduler");

            $filterStatus = $request->GET->get("status", null);
            $jobInfo = $scheduler->getJobDetails($jobId, $filterStatus);

            $tbl = phore_array_transform($jobInfo["tasks"], function ($key, $value) use ($jobId) {
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

            $retryBtnDisabled = " @disabled";
            $runTime = "--";
            if ($jobInfo["startTime"] != null) {
                $runTime = $jobInfo["startTime"];
                if ($jobInfo["endTime"] === null)
                    $runTime = time() - $runTime;
                else {
                    $runTime = $jobInfo["endTime"]- $runTime;
                }
            }
            if($jobInfo["status"] === PhoreSchedulerJob::STATUS_FAILED && $jobInfo["endTime"] === null) {
                $retryBtnDisabled = "";
            }
            $jobTbl = [
                (string)$jobInfo["jobId"],
                (string)$jobInfo["name"],
                (string)$jobInfo["status"],
                (string)$jobInfo["runAtTs"] == "" ? "--" : gmdate("Y-m-d H:i:s", (int) $jobInfo["runAtTs"]),
                (string)$jobInfo["startTime"] == "" ? "--" : gmdate("Y-m-d H:i:s", (int) $jobInfo["startTime"]),
                (string)$jobInfo["endTime"] == "" ? "--" : gmdate("Y-m-d H:i:s", (int) $jobInfo["endTime"]),
                (string)$runTime,
                (string)$jobInfo["continueOnFailure"] == 1 ? "true" : "false",
                (string)$jobInfo["nParallelTasks"],
                (string)$jobInfo["nPendingTasks"],
                (string)$jobInfo["nRunningTasks"],
                (string)$jobInfo["nFailedTasks"],
                (string)$jobInfo["nSuccessfulTasks"],
                fhtml(["a @href=? @btn @btn-danger".$retryBtnDisabled => "Retry"], ["{$this->startRoute}/scheduler?mode=retry&jobId={$jobInfo["jobId"]}"])
            ];


            $e = fhtml();
            $e[] = pt()->card(
                "Job Details for Job {$jobId}",
                pt("table-striped table-hover")->basic_table(
                    [
                        "Id", "Name", "Status", "Scheduled", "Start", "End", "Runtime",
                        "Continue on Failure", "Max Parallel Tasks", "Pending", "Running", "Failed", "Success", ""
                    ],
                    [$jobTbl],
                    ["",""]
                )
            );
            $filter = $filterStatus == null ? "none" : $filterStatus;
            $e[] = pt()->card(
                "Task list for job {$jobId}. Filter: {$filter}",
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

            $action = "";
            $jobId = $request->GET->get("jobId", false);
            if($jobId !== false) {
                $mode = $request->GET->get("mode", false);
                switch ($mode) {
                    case "cancel":
                        if($scheduler->cancelJob($jobId))
                            $action = "cancelled job $jobId.";
                        break;
                    case "retry":
                        if($scheduler->retryJob($jobId))
                            $action = "created new job from failed tasks.";
                        break;
                    case "del":
                        if($scheduler->deleteJob($jobId))
                            $action = "deleted job $jobId.";
                        break;
                }
            }

            $filterStatus = $request->GET->get("status", null);

            $jobList = $scheduler->getJobOverview($filterStatus);

            $tbl = phore_array_transform($jobList, function ($index, $ji) {
                $btnCancelDisabled = "";
                $btnDeleteDisabled = " @disabled";
                if($ji["status"] === PhoreSchedulerJob::STATUS_CANCELLED) {
                    $btnCancelDisabled = " @disabled";
                    $btnDeleteDisabled = "";
                }
                return [
                    $index + 1,
                    $ji["jobId"],
                    $ji["name"],
                    $ji["status"],
                    [
                        $ji["nTasks"] . "Tasks ( Pending:",
                        fhtml(["a @href=?"  => "{$ji["tasks_pending"]}"], ["{$this->startRoute}/scheduler/{$ji["jobId"]}?status=pending"]),
                        ", Running:",
                        fhtml(["a @href=?" => "{$ji["tasks_running"]}"], ["{$this->startRoute}/scheduler/{$ji["jobId"]}?status=running"]),
                        ", Failed:",
                        fhtml(["a @href=?" => "{$ji["nFailedTasks"]}"], ["{$this->startRoute}/scheduler/{$ji["jobId"]}?status=failed"]),
                        ", Success:",
                        fhtml(["a @href=?" => "{$ji["nSuccessfulTasks"]}"], ["{$this->startRoute}/scheduler/{$ji["jobId"]}?status=success"]),
                        ")"
                    ],
                    gmdate("Y-m-d H:i:s", (int) $ji["runAtTs"]) . "GMT",
                    [
                        fhtml(["a @href=? @btn @btn-primary" => "View"], ["{$this->startRoute}/scheduler/{$ji["jobId"]}"]),
                        fhtml(["a @href=? @btn @btn-danger".$btnCancelDisabled => "Cancel"], ["{$this->startRoute}/scheduler?mode=cancel&jobId={$ji["jobId"]}"]),
                        fhtml(["a @href=? @btn @btn-danger".$btnDeleteDisabled => "Del"], ["{$this->startRoute}/scheduler?mode=del&jobId={$ji["jobId"]}"])
                    ]
                ];
            });


            $e = fhtml();
            $e[] = pt()->card(
                "Scheduler $action",
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
