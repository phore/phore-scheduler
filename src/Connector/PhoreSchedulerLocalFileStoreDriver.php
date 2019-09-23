<?php


namespace Phore\Scheduler\Connector;


use Phore\FileSystem\PhoreFile;
use Phore\MicroApp\Type\PhoreSchedulerJob;
use Phore\MicroApp\Type\PhoreSchedulerTask;

class PhoreSchedulerLocalFileStoreDriver implements PhoreSchedulerConnector
{

    const FILTER_ALL = "all";
    const FILTER_SCHEDULED = "scheduled";

    /**
     * @var \Phore\FileSystem\PhoreFile
     */
    private $file;

    /**
     * @var PhoreSchedulerJob[]
     */
    private $jobs = null;

    /**
     * PhoreSchedulerLocalFileStoreDriver constructor.
     * @param $filename PhoreFile|string
     */
    public function __construct($filename)
    {
        $this->file = phore_file($filename);
        if ( ! $this->file->exists())
            $this->file->set_serialized([]);
    }

    public function __loadJobs()
    {
        if ($this->jobs !== null)
            return;

        $this->jobs = $this->file->get_serialized([PhoreSchedulerJob::class, PhoreSchedulerTask::class]);
    }


    /**
     * @param string $filter
     * @return PhoreSchedulerJob[]
     */
    public function getJobList($filter = self::FILTER_ALL): array
    {
        $ret = [];
        foreach ($this->jobs as $job) {
            $ret[] = $job;
        }
        return $ret;
    }

    protected function _getLockFile($jobId) : PhoreFile
    {
        return phore_file("/tmp/scheduler-lock-" . sha1($jobId));
    }

    public function tryLock($jobId) : bool
    {
        $lock = $this->_getLockFile($jobId);
        if ($lock->exists()) {
            return false;
        }
        $lock->touch();


    }


    public function unlock($jobId)
    {
        $lock = $this->_getLockFile($jobId);
        if ($lock->exists())
            $lock->unlink();
    }


    public function getJobInfo(string $jobId): array
    {
        // TODO: Implement getJobInfo() method.
    }

    public function createJob(string $jobId)
    {
        // TODO: Implement createJob() method.
    }
}