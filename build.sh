#!/bin/bash
#build jobsubmitter
mvn clean install

bin_dir=$JOBSUBMITTER_HOME"./bin"
logs_dir=$JOBSUBMITTER_HOME"./logs"
if [ ! -d "$bin_dir" ]; then 
rm -rf "./bin" 
fi
if [ ! -d "$logs_dir" ]; then 
rm -rf "./logs" 
fi 
mkdir ./bin

mv ./target/Jobsubmitter-1.0-SNAPSHOT.jar ./bin
rm -rf ./target
#build tools
echo "build LogAnalysiser"
javac -d . ./src/main/java/ict/util/LogAnalysis.java
jar cfm LogAnalysis.jar ./conf/MANIFEST.MF ./ict/util/*.class
rm -rf ./ict
mv ./LogAnalysis.jar ./bin
echo "done!"
#make log dir
mkdir ./logs
chmod +x ./tasks/cmd.sh
