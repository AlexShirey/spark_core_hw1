#!/usr/bin/bash

#local pathes
APP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
TMP_DIR=${APP_DIR}/tmp
JAR_DIR=${APP_DIR}/jar

#HDFS pathes
INPUT_DIR='/tmp/spark_core'
OUTPUT_DIR=/spark_core_output/$(date '+%Y-%m-%d_%H-%M-%S')

print_usage () {
	echo -e "\nusage: run.sh <bids file> <motels file> <exchanger_rates file> [source_file_zip]"
	echo -e "usage: [source_file_zip] - the path to the .zip archive with sources to compile and build new jar"
	echo -e "usage: if [source_file_zip] is not set, then the jar file from $APP_DIR/jar will be used\n"
	exit 1
}

# print help (usage)
if [[ $1 == "-help" ]]; then print_usage ; fi

#check the number of args
if ! [[ $# -eq 3 ]] && ! [[ $# -eq 4 ]]; then
	echo "error: incorrect number of args"
	print_usage
fi

#check if the input args are files
for arg in $1 $2 $3; do
	if [[ ! -f $arg ]]; then echo "$arg is not a file"; print_usage ; fi
done 

#create a directory where .jar file will be placed
if [[ ! -d $JAR_DIR ]]; then
	mkdir $JAR_DIR
fi	

#compile and build a jar file from sources if [source_file_zip] is set
if [[ $# -eq 4 ]]; then
	if [[ -f $4 && $4 == *.zip ]]; then
		echo -e "\nstarting build new jar from $4"
		mkdir $TMP_DIR
		unzip $4 -d $TMP_DIR
		mvn -f $TMP_DIR clean package
		if [[ $? -ne 0 ]]; then
			echo "error: problems occured while executing mvn clean package command, exiting..."
			exit 1
		fi 
		echo "file $(ls ${TMP_DIR}/target/*.jar) was built successfully, moving it to $JAR_DIR to execute on spark"
		rm -r -f $JAR_DIR/*
		mv ${TMP_DIR}/target/*.jar $JAR_DIR
		rm -r -f $TMP_DIR
	else 
		echo "error: $4 is not a zip archive"
		print_usage
	fi
fi

#check if the only one .jar file is in the app/jar dir
if [[ $(ls $JAR_DIR/*.jar | wc -l) -ne 1 ]]; then
	echo "error: directory $JAR_DIR must contain only one jar file to be executed"
	exit 1
fi

#.jar file to execute on spark
JAR_FILE=$(ls $JAR_DIR/*.jar)

#moving input files to HDFS
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put $1 ${INPUT_DIR}/bids.txt
hdfs dfs -put $2 ${INPUT_DIR}/motels.txt
hdfs dfs -put $3 ${INPUT_DIR}/rates.txt

echo "files in hdfs $INPUT_DIR"
hdfs dfs -ls $INPUT_DIR/

#starting spark on yarn to execute job
echo -e "\nall ok, starting spark using $JAR_FILE"
spark-submit --master yarn-client --driver-memory 2g --num-executors 3 --executor-memory 1g --conf spark.executor.cores=5 \
${JAR_FILE} ${INPUT_DIR}/bids.txt ${INPUT_DIR}/motels.txt ${INPUT_DIR}/rates.txt $OUTPUT_DIR

#remove unessesary input files from hdfs
hdfs dfs -rm -r -f $INPUT_DIR

echo -e "\nspark job is finished, check output hdfs directory $OUTPUT_DIR"
echo "execution of the app is finished." 



