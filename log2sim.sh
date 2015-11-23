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

info "Name of Workflow folder: $workflow_dir"
info "Check if directory exists in $log_dir ..."
if [ -d "$log_dir/$workflow_dir" ]
then
    info "\t$log_dir/$workflow_dir directory is found!"
else
    info "\t$log_dir/$workflow_dir directory is not accessible!"
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
$(ls -l ${log_dir}/${workflow_dir}/out/*.sh.out | wc -l) log files"

for log_file in  `ls ${log_dir}/${workflow_dir}/out/*.sh.out`; do
    info "\tParsing  $log_file ..."
    ./log_extractor.sh $log_file ${LFC_catalog}
done

###  Sanity checks  ###
# Assume that inputs/gate.sh.tar.gz, inputs/merge.sh.tar.gz, and
# inputs/opengate_version_7.0.tar.gz are at least stored on
# default SE. If not, add them    
if $(grep -q "gate_6.2_official_release" $LFC_catalog);
then
   sed 's/gate_6.2_official_release/opengate_version_7.0/g' -i $LFC_catalog
fi

if $(grep -q "release_Gate7.1_all" $LFC_catalog);
then
   sed 's/release_Gate7.1_all/opengate_version_7.0/g' -i $LFC_catalog
fi

if $(grep -q "xcheng_31_10_14_release" $LFC_catalog);
then
   sed 's/xcheng_31_10_14_release/opengate_version_7.0/g' -i $LFC_catalog
fi

if ! $(grep -q "opengate" $LFC_catalog); 
then
    echo "inputs/opengate_version_7.0.tar.gz,376927945,$defSE" >> $LFC_catalog
fi
if ! $(grep -q "gate.sh.tar.gz" $LFC_catalog); 
then
    echo "inputs/gate.sh.tar.gz,73043,$defSE" >> $LFC_catalog
fi
if ! $(grep -q "merge.sh.tar.gz" $LFC_catalog); 
then
    echo "inputs/merge.sh.tar.gz,90104445,$defSE" >> $LFC_catalog
fi

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
cmd="./mock_platform_generator.sh ${workflow_dir} initial"
info "\t$cmd"
$cmd
info "\tPlatform files: platform_${workflow_dir}_[avg/max]_[a/]symmetric.xml
 mock_platform_${workflow_dir}.xml ... created."

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
\t./ -> simulate_$workflow_dir.sh  Analysis_${workflow_dir}.Rmd README
\tcsv_files/ -> $db_dump $worker_nodes $file_transfer $se_bandwidth
\tsimgrid_files/ -> XML files and $LFC_catalog
\ttimings/ -> $real_times\n
To partially regenerate some files do:
\t../../scripts/deployment_generator.sh ${workflow_dir}
\t../../scripts/platform_generator.sh ${workflow_dir}
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
