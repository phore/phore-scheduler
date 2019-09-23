<?php
/**
 * Created by PhpStorm.
 * User: matthias
 * Date: 20.09.19
 * Time: 12:34
 */

namespace Phore\MicroApp\Type;

/**
 * Class PhoreSchedulerJob
 * @package Phore\MicroApp\Type
 * @internal
 */
class PhoreSchedulerJob
{

    public $jobId;

    public $status;

    /**
     * @var PhoreSchedulerTask[]
     */
    public $pendingTasks = [];

    /**
     * @var PhoreSchedulerTask[]
     */
    public $doneTasks = [];

}