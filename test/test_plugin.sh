#!/bin/sh -l

git clone --recursive https://github.com/hyrise/example_plugin/

cd example_plugin
./hyrise/install_dependencies.sh

mkdir cmake-build-debug && cd cmake-build-debug
cmake ..
make ExamplePlugin
cd ..

./cmake-build-debug/hyriseServer &
server_pid=$!

sleep 2

psql -h localhost -p 5432 -c "INSERT INTO meta_plugins(name) VALUES ('cmake-build-debug/libExamplePlugin.dylib')" | grep -q "SELECT 0"
ret=$?

kill -9 $server_pid
exit $ret