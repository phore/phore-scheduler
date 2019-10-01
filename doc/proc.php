<?php


$pids= [];
for ($i=0; $i<4; $i++) {

    $pid = pcntl_fork();

    if ($pid) {
        // Parent
        $pids[] = $pid;

    } else {
        while (1) {
            sleep(1);
        }
    }


}
while (count($pids) > 0) {
    $exitPid = pcntl_wait($status);
    $pids = array_diff($pids, [$exitPid]);
}