#!/bin/bash

FOLDER="rel"

numactl -m 2 -N 2 ninja -C $FOLDER Driver hyrisePlayground > /dev/null

PLUGIN=$(find $FOLDER -type f -name "libDriver.*" -exec basename {} \;)


#for BENCHMARK in "TPC-DS" "TPC-H" "JOB"
for BENCHMARK in "TPC-DS" "TPC-H"
do
	for ENCODING in "DictionaryFSBA" "DictionarySIMDBP128" "Unencoded" "LZ4" "RunLength" "FixedStringFSBAAndFrameOfReferenceFSBA" "FixedStringSIMDBP128AndFrameOfReferenceSIMDBP128"
	do
		/usr/bin/time -v --output=${BENCHMARK}__${ENCODING}.time sh -c "BENCHMARK_TO_RUN=${BENCHMARK} ENCODING_TO_USE=${ENCODING} numactl -m 2 -N 2 ./${FOLDER}/hyrisePlayground ${FOLDER}/${PLUGIN}"
#		retVal=$?
#		if [ $retVal -ne 0 ]; then
#j			echo "An error occured."
#			exit $retVal
#		fi
	done
done

