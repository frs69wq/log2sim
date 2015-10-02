#! /bin/bash -u

##############################################################################
# This script parses all log files related to the simulation of an           #
# application on the grid and creates corresponding files in CSV format.     #
# The log files are the log of executed jobs on the grid.                    #
# @Authors: Mohammad Mahdi BAZM, Frédéric SUTER                              #
# Company: CC-IN2P3 & CREATIS                                                #
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

info "Name of Workflow folder: $workflow_dir"
info "Check if directory exists in $log_dir ..."
if [ -d "$log_dir/$workflow_dir" ]
then
    info "\t$log_dir/$workflow_dir directory is found!"
else
    info "\t$log_dir/$workflow_dir directory is not accessible!"
    exit
fi    

worker_nodes="worker_nodes.csv"
file_transfer="file_transfer.csv"
real_times="real_times.csv"
se_bandwidth="se_bandwidth.csv"

# Set default SE name
defSE=$(awk -F'=' '/defSE/ {print $2}' configParser.txt)

# Set name of generated LFC catalog
LFC_catalog="LfcCatalog_$workflow_dir.csv"

# Set name of generated deployment file
deployment_file="deployment_$workflow_dir.xml"

info "Retrieving worker node names from database ..."

# Get database driver from config file
db_driver=$(awk -F'=' '/db_driver/ {print $2}' configParser.txt)

# Get the name of VIP database
db_name=$(basename ${log_dir}/${workflow_dir}/db/*.h2.db .h2.db)

# Create an SQL query to retrieve the names of worker from the VIP database
# Allows us to reconstruct broken names recovered from log files.
sql_get_jobs_info="SELECT ID, NODE_NAME, NODE_SITE,
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), QUEUED) as CREATION_TIME,
DATEDIFF('SECOND',QUEUED, DOWNLOAD) as QUEUING_TIME,
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), DOWNLOAD) as START_DOWNLOAD, 
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), UPLOAD) as START_UPLOAD 
from JOBS WHERE UPLOAD IS NOT NULL AND STATUS='COMPLETED' ORDER BY ID"

# Submit the SQL query to H2. 
# Redirect output in a temporary "host_names.txt" file.
java -cp ${db_driver} org.h2.tools.Shell \
     -url "jdbc:h2:${log_dir}/${workflow_dir}/db/$db_name" \
     -user gasw -password gasw -sql "$sql_get_jobs_info" > sql_results.txt \
|| info "SQL query failed."

sed s/' '//g -i sql_results.txt

##############################################################################
#                                                                            #
#                           log file extraction                              #
#                                                                            #
##############################################################################

# get number of files in the log directory in order to retrieve information.
info "Starting extraction from
$(ls -l ${log_dir}/${workflow_dir}/out/*.sh.out | wc -l) log files"

for log_file in  `ls ${log_dir}/${workflow_dir}/out/*.sh.out`; do
    info "\tParsing  $log_file ..."
    ./log_extractor.sh $log_file ${LFC_catalog} sql_results.txt
done

rm -f sql_results.txt

###  Sanity check ###
# Assume that inputs/gate.sh.tar.gz and
# inputs/opengate_version_7.0.tar.gz are at least stored on
# default SE. If not, add them    
if ! $(grep -q "opengate" $LFC_catalog); 
then
    echo "inputs/opengate_version_7.0.tar.gz,376927945,$defSE" >> $LFC_catalog
fi
if ! $(grep -q "gate.sh.tar.gz" $LFC_catalog); 
then
    echo "inputs/gate.sh.tar.gz,73043,$defSE" >> $LFC_catalog
fi

info "End of log file extraction."
info "\t Worker nodes: $worker_nodes ... created."
info "\t File transfers: $file_transfer ... created."
info "\t LFC initial catalog: $LFC_catalog ... created."
info "\t Job timings: real_times.csv ... created."


##############################################################################
#                                                                            #
#                          bandwidth computation                             #
#                                                                            #
##############################################################################

info "Computing SE bandwidth values ..."
cmd="./bandwidth_computer.sh initial"
info "\t$cmd"
$cmd
info "\tBandwidth file: $se_bandwidth ... created."

##############################################################################
#                                                                            #
#                             deployment file                                #
#                                                                            #
##############################################################################

info "Generating deployment file ..."

cmd="./deployment_generator.sh ${workflow_dir} initial"
info "\t$cmd"
$cmd
info "\tDeployment file: deployment_${workflow_dir}.xml ... created."

##############################################################################
#                                                                            #
#                             platform files                                 #
#                                                                            #
##############################################################################

info "Generating platform files ..."

cmd="./platform_generator.sh ${workflow_dir} initial"
info "\t$cmd"
$cmd
info "\tPlatform files: platform_${workflow_dir}_[avg/max]_[a/]symmetric.xml
 ... created."

##############################################################################
#                                                                            #
#                          simulation launcher                               #
#                                                                            #
##############################################################################

info "Generating simulation launcher ..."
cmd="./launcher_generator.sh $workflow_dir cheat"
info "\t$cmd"
$cmd
info "\tLauncher: simulate_$workflow_dir.sh ... created."

##############################################################################
#                                                                            #
#                              README file                                   #
#                                                                            #
##############################################################################
echo -e "Data for $workflow_dir originally produced on: "$(date +"%D %T")"\n
Directory organization:
\t./ -> simulate_$workflow_dir.sh ${workflow_dir}_summary.html Analysis_${workflow_dir}.Rmd README
\tcsv_files/ -> $worker_nodes $file_transfer $se_bandwidth
\tsimgrid_files/ -> XML files and $LFC_catalog
\ttimings/ -> $real_times\n
To partially regenerate some files do:
\t../../scripts/bandwidth_computer.sh
\t../../scripts/deployment_generator.sh ${workflow_dir}
\t../../scripts/platform_generator.sh ${workflow_dir}\n" > \
README

##############################################################################
#                                                                            #
#                             HTML Summary                                   #
#                                                                            #
##############################################################################
info "Generating an HTML Summary ..."
cmd="./summary_generator.sh $workflow_dir"
info "\t$cmd"
$cmd
info "\tHTML Summary: ${workflow_dir}_summary.html ... created"

##############################################################################
#                                                                            #
#                             Analysis File                                  #
#                                                                            #
##############################################################################
sed s/WORKFLOW_NAME/$workflow_dir/g Analysis.Rmd > Analysis_$workflow_dir.Rmd

##############################################################################
#                                                                            #
#                             Moving files                                   #
#                                                                            #
##############################################################################

info "Moving produced files in ../results/$workflow_dir"
output_dir="../results/$workflow_dir"
if [ ! -d $output_dir ]
then
    mkdir $output_dir
    mkdir $output_dir/csv_files
    mkdir $output_dir/simgrid_files
    mkdir $output_dir/timings
fi
info "\t$output_dir -> simulate_$workflow_dir.sh"\
     " ${workflow_dir}_summary.html README Analysis_${workflow_dir}.Rmd"
info "\t$output_dir/csv_files/ -> $worker_nodes $file_transfer $se_bandwidth"
info "\t$output_dir/simgrid_files/ -> XML files and $LFC_catalog"
info "\t$output_dir/timings/ -> $real_times"

mv -f simulate_*.sh *.html Analysis_$workflow_dir.Rmd README $output_dir/
mv -f *.xml  $LFC_catalog $output_dir/simgrid_files
mv -f $worker_nodes $file_transfer $se_bandwidth $output_dir/csv_files
mv -f $real_times $output_dir/timings

#rm -f sql_results.txt
#Generate application file.
#  info "Generating application file ..."
#  FLE_APPLICATION="Application_${workflow_dir}.txt"
#  ./gen_application_file.sh ${workflow_dir} ${file_transfer} \
# 	${FLE_APPLICATION}
#  info "Application file: $FLE_APPLICATION created."
#    mv -f Application_*.txt 
