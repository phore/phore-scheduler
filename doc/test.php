<?php


$fd = function() {
    echo "wurst";
};

$ref = new ReflectionFunction($fd);

echo REflection::export($ref, true);