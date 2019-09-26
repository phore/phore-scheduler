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

        $app->addPage("{$this->startRoute}/scheduler", function (App $app) {

            $scheduler = $app->get($this->diName);
            if ( ! $scheduler instanceof PhoreScheduler)
                throw new \InvalidArgumentException("{$this->diName} schould be from type PhoreScheduler");


            $table = new Table(["JobId", "Name", "Tasks", "Status"]);
            $jobInfo = $scheduler->getJobInfo();


            $tbl = phore_array_transform($jobInfo, function ($index, $ji) {

                return [
                    $index + 1,
                    $ji["jobId"],
                    $ji["name"],
                    $ji["status"],
                    $ji["tasks_all"] . " Tasks (Pending: {$ji["tasks_pending"]}, Success: {$ji["tasks_ok"]}, Failed: {$ji["tasks_failed"]})"
                ];
            });


            $e = fhtml();
            $e[] = pt()->card(
                "Scheduler",
                pt("table-striped table-hover")->basic_table(
                    ["#", "JobID", "Job Name", "Status", "Task Status"],
                    $tbl,
                    ["","",""]
                )

            );

            return $e;


        },  new NaviButtonWithIcon("Scheduler", "fas fa-clock"));
    }

}
