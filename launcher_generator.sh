#! /bin/bash -u

##################################################################
# This script generates a simulation execution file in bash      #
# for runnig the simulation of Simgrid.                          #
# @author:Mohammad Mahdi BAZM                                    #
# Company: CC-IN2P3 & CREATIS laboratory                         #
##################################################################
#Get the path of logs folder that contain all workflow folders.
log_dir=$(awk -F'=' '/log_folder/ {print $2}' configParser.txt)
sim_dir=$(awk -F'=' '/sim_folder/ {print $2}' configParser.txt)

workflow_dir=${1:? name of workflow must passed as argument!}
cheat=${2:-"no"}

output="simulate_$workflow_dir.sh"

deployment_file="deployment_$workflow_dir.xml"

total_particle_number=$(awk '/] Initial number of particles:/''{print $NF}' \
    ${log_dir}/${workflow_dir}/workflow.out)

number_of_gate_jobs=$(awk '/] processor "gate" executed/''{print}' \
    ${log_dir}/${workflow_dir}/workflow.out | awk 'END{print}' | awk '{print $(NF-1)}')

if [ $cheat != "no" ]
then 
    total_particle_number=$number_of_gate_jobs
fi

gate_input_file=$(awk -F',' '/zip/ {gsub("inputs/","",$1); gsub(".zip","", $1); print $1}' LfcCatalog_$workflow_dir.csv) 

sos_time=300

number_of_merge_jobs=$(awk '/] processor "merge" executed/''{print}' \
    ${log_dir}/${workflow_dir}/workflow.out | awk 'END{print}' | awk '{print $(NF-1)}')

if [ $cheat != "no" ]
then
  cpu_merge_time=0 
  events_per_sec=0
else
  cpu_merge_time=10 
  events_per_sec=200
  # event_per_sec=$(awk '/Average of Computational cost/ {print $NF}' $application_file)
fi
  
log_file=$workflow_dir"_log.txt"



# Writing informations to the output file.

echo '#! /bin/bash -u' > $output

echo -e '# Command lines arguments are:\n' \
        '# Platform files: platform_'$workflow_dir'_[max/av]_[a/]symmetric.xml\n' \
        '# Deployment file: '$deployment_file'\n' \
        '# Initial number particles: '$total_particle_number'\n' \
        '# Number of gate jobs: '$number_of_gate_jobs'\n' \
        '# Gate input file: '$gate_input_file'\n' \
        '# SoS time: '$sos_time'\n' \
        '# Number of merge jobs: '$number_of_merge_jobs'\n' \
        '# CPU merge time: '$cpu_merge_time'\n' \
        '# Events per second: '$events_per_sec'\n' \
        '# Log file: '$log_file'\n' >> $output 

echo -e ' verbose=${1:-""}\n'\
        'if [[ $verbose == "-v" ]]\n'\
        'then\n'\
        '\tverbose="'"--log=root.fmt:[%12.6r]%e(%3i:%10P@%40h)%e%m%n"'"\n'\
        'else\n'\
        '\tverbose="'"--log=jmsg.thres:critical"'"\n'\
        'fi\n' >> $output

# Order of argument: Platform Deployment TotalParticleNumber NmuberOfGateJob SOSTime NumberOfMergeJob cpuMergeTime eventsPerSec LogFile
for platform_type in "max_symmetric" "max_asymmetric" "avg_symmetric" "avg_asymmetric"
do  
    echo "echo Simulate on '$platform_type'" >>$output
    echo  'java -cp '${sim_dir}'/bin:/usr/local/java/simgrid.jar VIPSimulator \
      simgrid_files/platform_'${workflow_dir}'_'${platform_type}'.xml simgrid_files/'${deployment_file}' \
      '${total_particle_number}' '${number_of_gate_jobs}' '${gate_input_file}' '${sos_time}' '${number_of_merge_jobs}' '${cpu_merge_time}' '${events_per_sec}'\
      '${log_file}' ${verbose}  > timings/simulated_time_on_'${platform_type}'.csv'  >> $output 

    echo -e "\n" >> $output
done 
#give execution right to the generated file in .sh
chmod +x $output
