#!/bin/bash

FOLDER="rel"

numactl -m 3 -N 3 ninja -C $FOLDER Driver hyrisePlayground 2>&1 >/dev/null

PLUGIN=$(find $FOLDER -type f -name "libDriver.*" -exec basename {} \;)


for BENCHMARK in "TPC-DS" "TPC-H" "JOB" "TPC-C"
#for BENCHMARK in "TPC-C"
do
	for ENCODING in "DictionaryFSBA" "DictionarySIMDBP128" "Unencoded" "LZ4" "RunLength" "FixedStringFSBAAndFrameOfReferenceFSBA" "FixedStringSIMDBP128AndFrameOfReferenceSIMDBP128"
	do
		/usr/bin/time -v --output=${BENCHMARK}__${ENCODING}.time sh -c "BENCHMARK_TO_RUN=${BENCHMARK} ENCODING_TO_USE=${ENCODING} numactl -m 3 -N 3 ./${FOLDER}/hyrisePlayground ${FOLDER}/${PLUGIN}"
#		retVal=$?
#		if [ $retVal -ne 0 ]; then
#j			echo "An error occured."
#			exit $retVal
#		fi
	done
done

