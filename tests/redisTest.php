<?php

namespace Test;

use PHPUnit\Framework\TestCase;

class redisTest extends TestCase
{
    public function testRedis()
    {
        $redis = new \Redis();
        $redis->connect("redis");
        $redis->flushAll();

        $redis->set("testkey", "val1");
        $result = $redis->get("testkey");
        $this->assertEquals("val1", $result);
        $redis->set("testkey", "val2");
        $result = $redis->get("testkey");
        $this->assertEquals("val2", $result);
        $redis->set("testkey", "val3");
        $result = $redis->get("testkey");
        $this->assertEquals("val3", $result);

    }

}
