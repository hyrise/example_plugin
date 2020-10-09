ninja -C rel Driver MvccDeletePlugin hyrisePlayground

BENCHMARK_TO_RUN=CH ./rel/hyrisePlayground ./rel/libDriver.dylib &
CH_PID=$!

sleep 400
echo 'Killing it'
kill $CH_PID
