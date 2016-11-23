#! /bin/bash -u
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER, Mohammad Mahdi BAZM (2015)                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

workflow_dir=${1:? name of workflow must passed as argument!}
cheat=${2:-"no"}
initial=${3:-"standalone"}

output="simulate_${workflow_dir}_2.sh"
deployment_file="deployment_$workflow_dir.xml"
deployment_file2="deployment_${workflow_dir}_2.xml"

if [ $initial == "initial" ]
then 
    db_dump="db_dump.csv"
else
    db_dump="csv_files/db_dump.csv"
    echo -e [`date +"%D %T"`] "Launcher file regeneration" >> README
fi

number_of_gate_jobs=$(grep gate $db_dump |wc -l)
# awk '/] processor "gate" executed/''{print}' \
#     ${LOG2SIM_LOGS}/${workflow_dir}/workflow.out | awk 'END{print}' | \
#     awk '{print $(NF-1)}')

# total_particle_number=$(awk '/] Initial number of particles:/''{print $NF}' \
#     ${LOG2SIM_LOGS}/${workflow_dir}/workflow.out)

if [ $cheat != "no" ]
then 
    total_particle_number=$number_of_gate_jobs
    number_of_merge_jobs=1
fi

sos_time=300

# number_of_merge_jobs=$(awk '/] processor "merge" executed/''{print}' \
#     ${LOG2SIM_LOGS}/${workflow_dir}/workflow.out | awk 'END{print}' | \
#     awk '{print $(NF-1)}')

if [ $cheat != "no" ]
then
  cpu_merge_time=0 
  events_per_sec=0
else
  cpu_merge_time=10 
  events_per_sec=200
  # event_per_sec=$(awk '/Average of Computational cost/ {print $NF}' $application_file)
fi
  
# Writing informations to the output file.

echo '#! /bin/bash -u' > $output

echo -e '# Command lines arguments are:\n' \
        '# Platform files:  platform_'$workflow_dir'_x_x_lim.xml\n'\
        '# Deployment file: '$deployment_file2'\n' \
        '# Initial number particles: '$total_particle_number'\n' \
        '# Number of gate jobs: '$number_of_gate_jobs'\n' \
        '# SoS time: '$sos_time'\n' \
        '# Number of merge jobs: '$number_of_merge_jobs'\n' \
        '# CPU merge time: '$cpu_merge_time'\n' \
        '# Events per second: '$events_per_sec'\n' \
        '# version: 1 or 2\n' >> $output 

echo -e 'platform_type=${1:-"all"}\n'>> $output
echo -e 'verbose=${2:-""}\n'\
        'if [[ $verbose == "-v" ]]\n'\
        'then\n'\
        '\tverbose="'"--log=root.fmt:[%12.6r]%e(%3i:%10P@%40h)%e%m%n"'"\n'\
        'else\n'\
        '\tverbose="'"--log=jmsg.thres:critical"'"\n'\
        'fi\n' >> $output


echo -e 'flag="--cfg=network/crosstraffic:0"'>> $output
# Order of argument: Platform Deployment TotalParticleNumber NmuberOfGateJob SOSTime NumberOfMergeJob cpuMergeTime eventsPerSec LogFile

echo 'cmd="java -cp ${VIPSIM}/bin:${SIMGRID_PATH}/simgrid.jar \
 VIPSimulator"'>> $output
echo 'params="simgrid_files/'${deployment_file}' \
 '${total_particle_number}' '${number_of_gate_jobs}' '${sos_time}' '${number_of_merge_jobs}' '${cpu_merge_time}' '${events_per_sec}' ${version} 10000000 ${verbose}"' >> $output

echo -e "\n" >> $output


echo -e 'version=3\n' >> $output
echo 'for platform in "10G_SE" "Avg_SE" "Max_SE" "Asym_Avg_SE" "Asym_Max_SE" "Avg_lim" "Max_lim" "Corr_Max_lim" "Corr_Max_cluster" "ultimate"
    do
      echo -e "\\tSimulate on AS  - version ${version}" 
      platform_file="simgrid_files/platform_'${workflow_dir}'_${platform}.xml"
      run=$cmd" ${platform_file} simgrid_files/'${deployment_file2}' '${total_particle_number}' '${number_of_gate_jobs}' '${sos_time}' '${number_of_merge_jobs}' '${cpu_merge_time}' '${events_per_sec}' ${version} 10000000 ${verbose} ${flag}"
      echo -e "\\t\\t$run"
      $run  1> timings/simulated_time_on_${platform}.csv \
            2> csv_files/simulated_file_transfer_on_${platform}.csv
      sed -i '1d' csv_files/simulated_file_transfer_on_${platform}.csv        
    done' >> $output






#give execution right to the generated file in .sh
chmod +x $output
