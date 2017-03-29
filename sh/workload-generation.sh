#!/bin/bash
echo "compile assembly task of CloudArch"
bak=".o"
memdirname="mem"
taskfolder=$JOBSUBMITTER_HOME"/tasks/assembly/"  
for file_a in ${taskfolder}/*; do  
    temp_file=`basename $file_a`  
    firstname=${temp_file%.*}
    oname=${firstname}${bak}
    if [[ temp_file != memdirname ]]; then
	gcc -c $taskfolder$temp_file -o $taskfolder/$oname
        gcc $taskfolder$oname -o $taskfolder/$firstname
    fi;
done   
gcc $taskfolder"memorycore.c" -o $taskfolder/"memorycore"    
echo "done"
