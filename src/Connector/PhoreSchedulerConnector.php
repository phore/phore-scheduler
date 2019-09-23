<?php


namespace Phore\Scheduler\Connector;


use Phore\MicroApp\Type\PhoreSchedulerJob;

interface PhoreSchedulerConnector
{

    /**
     * @return PhoreSchedulerJob[]
     */
    public function getJobList() : array;

    public function getJobInfo(string $jobId) : array;

    public function tryLock(string $jobId) : bool;

    public function unlock(string $jobId);

    public function createJob(string $jobId );


}