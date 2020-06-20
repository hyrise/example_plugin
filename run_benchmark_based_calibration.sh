#!/bin/bash

FOLDER="rel"

ninja -C $FOLDER Driver hyrisePlayground > /dev/null

PLUGIN=$(find $FOLDER -type f -name "libDriver.*")

for BENCHMARK in "TPC-DS" "TPC-H" "JOB"
do
	for ENCODING in "DictionaryFSBA" "DictionarySIMDBP128" "Uncompressed" "LZ4" "RunLength" "FixedStringAndFrameOfReferenceFSBA" "FixedStringAndFrameOfReferenceSIMDBP128"
	do
	/usr/bin/time -v --output=${BENCHMARK}__${ENCODING}.time sh -c "BENCHMARK_TO_RUN=${BENCHMARK} ENCODING_TO_USE=${ENCODING} ./${FOLDER}/hyrisePlayground ${FOLDER}/${PLUGIN}"
	done
done