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
    echo "JobId,Command,Name,Site,CreationTime, QueuingDuration,"\
"DownloadStartTime,DownloadDuration,ComputeStartTime,ComputeDuration"\
"UploadStartTime,UploadDuration,TotalDuration" > $output
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
DATEDIFF('SECOND',DOWNLOAD,END_E) as TOTAL_TIME
from JOBS WHERE STATUS='COMPLETED' ORDER BY ID"


java -cp ${db_driver} org.h2.tools.Shell \
     -url "jdbc:h2:${log_dir}/${workflow_dir}/db/jobs" \
     -user gasw -password gasw -sql "$sql_query" | \
     sed -e '1d' -e '$d' -e 's/ *| */ /g' >> $output \
|| info "SQL query failed."

