#! /bin/bash -u
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER, Mohammad Mahdi BAZM (2015)                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

# local function to log the steps of execution of the script
function info {
  echo -e [`date +"%D %T"`] $*
}

#Get the path of logs folder that contain all workflow folders.
log_dir=$(awk -F'=' '/log_folder/ {print $2}' configParser.txt)

# Get the name of the directory that contains all the log files
# related to a workflow. This directory is located $log_dir.
workflow_dir=${1:? Workflow directory name is mandatory!}

# Get database driver from config file
db_driver=$(awk -F'=' '/db_driver/ {print $2}' configParser.txt)
output="db_dump.csv"

if [ ! -f "$output" ]
then
    info "File $output does not exist. Create it."
    echo "JobId Command Name Site CreationTime QueuingDuration "\
"DownloadStartTime DownloadDuration ComputeStartTime ComputeDuration "\
"UploadStartTime UploadDuration TotalDuration logFile" > $output
fi

# Prepare the SQL query
sql_query="SELECT ID, COMMAND, NODE_NAME, NODE_SITE,
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), QUEUED) as CREATION_TIME,
DATEDIFF('SECOND',QUEUED, DOWNLOAD) as QUEUING_TIME,
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), DOWNLOAD) as START_DOWNLOAD,
DATEDIFF('SECOND',DOWNLOAD,RUNNING) as DOWNLOAD_TIME,
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), RUNNING) as START_COMPUTE,
DATEDIFF('SECOND',RUNNING,UPLOAD) as COMPUTE_TIME,
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), UPLOAD) as START_UPLOAD,
DATEDIFF('SECOND',UPLOAD, END_E) as UPLOAD_TIME,
DATEDIFF('SECOND',DOWNLOAD,END_E) as TOTAL_TIME, FILE_NAME
from JOBS WHERE STATUS='COMPLETED' ORDER BY ID"


java -cp ${db_driver} org.h2.tools.Shell \
     -url "jdbc:h2:${log_dir}/${workflow_dir}/db/db/jobs" \
     -user gasw -password gasw -sql "$sql_query" | \
     sed -e '1d' -e '$d' -e 's/ *| */ /g' >> $output \
|| info "SQL query failed."

sed '1d' $output | while read line
do
    fields=$(echo $line | awk '{print $1,$2,$3,$4,$5,$6,$7,$NF}')
    set -- $fields
    # $1 = job id
    # $2 = command
    # $3 = worker name
    # $4 = grid site
    # $5 = creation time
    # $6 = queuing time
    # $7 = download start time
    # $8 (in fields) = $NF (in line) = log file name
    
    site=$(echo $4 | tr '[:lower:]' '[:upper:]')
    sed "s/$4/$site/g" -i $output
    
    if [[ $3 == NULL ]] || [[ $3 == null ]] 
    then	
	input_log=$log_dir/$workflow_dir/out/$8.sh.out
	info "\tBad entry for job $1. Look to " \
	    "${input_log} to correct it".
	machine_name=$(awk -F'=' '/^HOSTNAME/ {print $NF}' $input_log)
	site=$(awk -F'=' '/^SITE_NAME/ {print $NF}' $input_log |\
               tr '[:lower:]' '[:upper:]')
	
	if [[ $machine_name == "" ]]
	then
	    # means the log file is absent
	    info "\tMissing log file, discard this job"
	    sed "/$line/d" -i $output
	else
	    download_duration=$(awk '/] Input download/''{print}' $input_log | \
		awk '{print $(NF-1)}')
	    compute_time=$(awk '/] Execution time:/''{print}' $input_log | \
		awk '{print $(NF-1)}')
	    upload_duration=$(awk '/] Results upload/''{print}' $input_log | \
		awk '{print $(NF-1)}')    
	    total_time=$(awk '/] Total running/''{print}' $input_log | \
		awk '{print $(NF-1)}')
	    compute_start=$(($6 + $download_duration))
	    upload_start=$(($compute_start + $compute_time))
	    new_line=$(echo -e $1 $2 $machine_name $site $5 $6 $7 \
		$download_duration $compute_start $compute_time $upload_start \
		$upload_duration $total_time $8)
	    sed "s/$line/$new_line/g" -i $output
	fi
    fi
done
