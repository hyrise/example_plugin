#!/bin/bash

FOLDER="rel"

numactl -m 3 -N 3 ninja -C $FOLDER Driver hyrisePlayground 2>&1 >/dev/null

PLUGIN=$(find $FOLDER -type f -name "libDriver.*" -exec basename {} \;)

trap exit SIGINT;


for BENCHMARK in "TPC-DS" "TPC-H" "JOB" "TPC-C"
#for BENCHMARK in "TPC-C"
do
	for i in {0..5}
	do
		/usr/bin/time -v --output=${BENCHMARK}__random_config__${i}.time sh -c "BENCHMARK_TO_RUN=${BENCHMARK} ENCODING_CONFIG=random_configs/random_config__${BENCHMARK}__${i}.json numactl -m 3 -N 3 ./${FOLDER}/hyrisePlayground ${FOLDER}/${PLUGIN}"
		# retVal=$?
		# if [ $retVal -ne 0 ]; then
		# 	echo "An error occured."
		# 	exit $retVal
		# fi
	done

	for ENCODING in "DictionaryFSBA" "DictionarySIMDBP128" "Unencoded" "LZ4" "RunLength" "FixedStringFSBAAndFrameOfReferenceFSBA" "FixedStringSIMDBP128AndFrameOfReferenceSIMDBP128"
	do
		/usr/bin/time -v --output=${BENCHMARK}__${ENCODING}.time sh -c "BENCHMARK_TO_RUN=${BENCHMARK} ENCODING_TO_USE=${ENCODING} numactl -m 3 -N 3 ./${FOLDER}/hyrisePlayground ${FOLDER}/${PLUGIN}"
		# retVal=$?
		# if [ $retVal -ne 0 ]; then
		# 	echo "An error occured."
		# 	exit $retVal
		# fi
	done
done

