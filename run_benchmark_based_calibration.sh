#!/bin/bash

FOLDER="rel"

ninja -C $FOLDER Driver hyrisePlayground > /dev/null

PLUGIN=$(find $FOLDER -type f -name "libDriver.*" -exec basename {} \;)


for BENCHMARK in "TPC-DS" "TPC-H" "JOB"
do
	for ENCODING in "DictionaryFSBA" "DictionarySIMDBP128" "Unencoded" "LZ4" "RunLength" "FixedStringFSBAAndFrameOfReferenceFSBA" "FixedStringSIMDBP128AndFrameOfReferenceSIMDBP128"
	do
	/usr/bin/time -v --output=${BENCHMARK}__${ENCODING}.time sh -c "BENCHMARK_TO_RUN=${BENCHMARK} ENCODING_TO_USE=${ENCODING} ./${FOLDER}/hyrisePlayground ${FOLDER}/${PLUGIN}"
	done
done

