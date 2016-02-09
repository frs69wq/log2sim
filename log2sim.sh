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

# Get the name of the directory that contains all the log files
# related to a workflow. This directory is located $LOG2SIM_LOGS.
workflow_dir=${1:? Workflow directory name is mandatory!}

info "Name of Workflow folder: $workflow_dir"
info "Check if directory exists in $LOG2SIM_LOGS ..."
if [ -d "$LOG2SIM_LOGS/$workflow_dir" ]
then
    info "\t$LOG2SIM_LOGS/$workflow_dir directory is found!"
else
    info "\t$LOG2SIM_LOGS/$workflow_dir directory is not accessible!"
    exit
fi    

db_dump="db_dump.csv"
worker_nodes="worker_nodes.csv"
file_transfer="file_transfer.csv"
real_times="real_times.csv"
se_bandwidth="se_bandwidth.csv"

# Set default SE name
defSE="ccsrm02.in2p3.fr"

# Set name of generated LFC catalog
LFC_catalog="LfcCatalog_$workflow_dir.csv"

# Set name of generated deployment file
deployment_file="deployment_$workflow_dir.xml"

##############################################################################
#                                                                            #
#                           database extraction                              #
#                                                                            #
##############################################################################

info "Starting extraction from job database"
./db_extractor.sh $workflow_dir
info "End of database extraction."
info "\t DB dump: $db_dump ... created."

##############################################################################
#                                                                            #
#                           log file extraction                              #
#                                                                            #
##############################################################################

# get number of files in the log directory in order to retrieve information.
info "Starting extraction from
$(ls -l ${LOG2SIM_LOGS}/${workflow_dir}/out/*.sh.out | wc -l) log files"

for log_file in  `ls ${LOG2SIM_LOGS}/${workflow_dir}/out/*.sh.out`; do
    info "\tParsing  $log_file ..."
    ./log_extractor.sh $log_file ${LFC_catalog}
done

# Checking the host name format. 
# Test if the suffix of the host name has a match in internet_suffixes.txt
# If it misses a proper suffix, we use the VIP database to complete the name.
sed '1d' $worker_nodes | while read line
do
    worker_name=$(echo $line | awk -F',' '{print $2}')
    suffix=$(echo $worker_name | awk -F '.' '{print $NF}')
    if ! grep -q "$suffix" internet_suffixes.txt ; then
	new_name=$(grep -w $worker_name $db_dump | awk '{print $3}' | uniq)
	new_suffix=$(echo $new_name | awk -F'.' '{print $NF}')
	
	new_line=$(echo $line | awk -F',' -v s=$new_suffix -v n=$new_name \
	    '{sub($7,s,$7); sub($2,n,$2); gsub(" ",",",$0); print $0}')
	sed "s/$line/$new_line/g" -i $worker_nodes
	sed "s/$worker_name/$new_name/g" -i $file_transfer
    fi
done
# remove lines with missing information in file transfers
sed '/,,/d' -i $file_transfer

info "End of log file extraction."
info "\t Worker nodes: $worker_nodes ... created."
info "\t File transfers: $file_transfer ... created."
info "\t LFC initial catalog: $LFC_catalog ... created."
info "\t Job timings: real_times.csv ... created."

##############################################################################
#                                                                            #
#                             deployment file                                #
#                                                                            #
##############################################################################

info "Generating deployment files ..."

cmd="./deployment_generator.sh ${workflow_dir} initial"
info "\t$cmd"
$cmd
cmd="./deployment_generator.R ${workflow_dir} initial"
info "\t$cmd"
$cmd
info "\tDeployment file: deployment_${workflow_dir}*.xml ... created."

##############################################################################
#                                                                            #
#                             platform files                                 #
#                                                                            #
##############################################################################

info "Generating platform files ..."

cmd="./platform_generator.sh ${workflow_dir} initial"
info "\t$cmd"
$cmd
cmd="./platform_generator.R ${workflow_dir} initial"
info "\t$cmd"
$cmd
cmd="./mock_platform_generator.sh ${workflow_dir} initial"
info "\t$cmd"
$cmd
info "\tPlatform files: platform_${workflow_dir}_[avg/max]_[a/]symmetric.xml
 [AS/mock]_platform_${workflow_dir}.xml ... created."

##############################################################################
#                                                                            #
#                          simulation launcher                               #
#                                                                            #
##############################################################################

info "Generating simulation launcher ..."
cmd="./launcher_generator.sh $workflow_dir cheat initial"
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
\t./ -> simulate_$workflow_dir.sh  Analysis_${workflow_dir}.Rmd README
\tcsv_files/ -> $db_dump $worker_nodes $file_transfer $se_bandwidth
\tsimgrid_files/ -> XML files and $LFC_catalog
\ttimings/ -> $real_times\n
To partially regenerate some files do:
\t../../scripts/deployment_generator.sh ${workflow_dir}
\t../../scripts/deployment_generator.R ${workflow_dir}
\t../../scripts/platform_generator.sh ${workflow_dir}
\t../../scripts/platform_generator.R ${workflow_dir}
\t../../scripts/launcher_generator.sh ${workflow_dir}
\t../../scripts/mock_generator.sh ${workflow_dir}\n" > \
README

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
     " README Analysis_${workflow_dir}.Rmd"
info "\t$output_dir/csv_files/ -> $db_dump $worker_nodes $file_transfer $se_bandwidth"
info "\t$output_dir/simgrid_files/ -> XML files and $LFC_catalog"
info "\t$output_dir/timings/ -> $real_times"

mv -f simulate_*.sh Analysis_$workflow_dir.Rmd README $output_dir/
mv -f *.xml  $LFC_catalog $output_dir/simgrid_files
mv -f $db_dump $worker_nodes $file_transfer $se_bandwidth $output_dir/csv_files
mv -f $real_times $output_dir/timings
