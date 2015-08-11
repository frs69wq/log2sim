#!/bin/bash -u

##################################################################
# This script generates an application file in format txt for    #
# using in Simgrid. Information will be retrieved from log files #
# and DB of Workflow.                                            #
# @author:Mohammad Mahdi BAZM                                    #
# Company: CC-IN2P3 & CREATIS laboratory                         #
##################################################################
# local function to log the steps of execution of the script
function info {
  echo [`date +"%D %T"`] $*
}

#Get the path of logs folder that contain all workflow folders.
log_dir=$(awk -F'=' '/log_folder/ {print $2}' configParser.txt)

#Input parameter is the name of folder that contains all of log files and DB file related to a workflow
workflow_dir=${1:? "Name of workflow folder is mandatory!!"}
transfer_info=${2:? "Name of the file that contains transfers is mandatory!!"}
output=${3:-application.txt}

#Number of jobs (gate and merge) using workflow.out
num_gate_jobs=$(awk '/] processor "gate" executed/''{print}' ${log_dir}/${workflow_dir}/workflow.out | awk 'END{print}' | awk '{print $(NF-1)}')
num_merge_jobs=$(awk '/] processor "merge" executed/''{print}' ${log_dir}/${workflow_dir}/workflow.out | awk 'END{print}' | awk '{print $(NF-1)}')
echo "Number of GATE jobs: $num_gate_jobs" > $output
echo "Number of Merge jobs: $num_merge_jobs" >> $output

# Get number of initial particles from workflow.out
num_initial_particules=$(awk '/] Initial number of particles:/''{print $NF}' ${log_dir}/${workflow_dir}/workflow.out)
echo "Initial number of particules: $num_initial_particules" >> $output

######################  info of Gate jobs ######################################################
#initialize variables
gate_total_event=0
merge_total_event=0
gate_total_comput_cost=0
merge_total_comput_cost=0

echo "*************Computational cost of Gate jobs***********" >> $output
echo "--------------------------------------------------------" >> $output
echo "|JOB ID,NBR EVENTS,EXE_TIME,BOGOMIPS,COMPUTATIONAL COST|" >> $output
echo "--------------------------------------------------------" >> $output
for fle in ${log_dir}/${workflow_dir}/out/gate*.sh.out; do
    chk_file=$(awk '/] Total running time:/''{print}' $fle)
    if [ "$chk_file" != "" ]
    then
        gate_exe_time=$(awk '/] Total running time:/''{print}' $fle | awk '{print $(NF-1)}') 

        gate_num_event=$(awk '/^G[a-zA-Z][a-zA-Z][a-zA-Z] finished/ {print $4}' $fle)
        if [[ "$gate_num_event" == "" ]];then
            gate_num_event=$(awk '/# NumberOfEvents =/''{print}' $fle | awk 'END{print}' | awk '{print $NF}')
            if [[ "$gate_num_event" == "" ]];then
		gate_num_event=$(grep -r finalmsg $fle | awk -F',' '{print $NF}' | awk '{gsub(/[\47*\]]/,"");print $0}')
	    fi
        fi
        
        cpu_bogomips=$(awk '/bogomips/ {print $NF}' $fle | uniq | awk 'NR==1')
        gate_job_id=$(awk -F'=' '/^JOBID=/''{print $2}' $fle)
        gate_computational_cost=`echo "$cpu_bogomips * $gate_exe_time / $gate_num_event" | bc -l`
        gate_total_comput_cost=`echo "$gate_total_comput_cost + $gate_computational_cost" | bc -l`
        gate_total_event=`echo "$gate_total_event + $gate_num_event" | bc`
        echo "$gate_job_id,$gate_num_event,$gate_exe_time,$cpu_bogomips,$gate_computational_cost" >> $output
    fi
done

echo "--------------------------------------------------------" >> $output

echo "Gate total events = $gate_total_event" >> $output
echo "Gate average Computational cost : `echo "$gate_total_comput_cost / $num_gate_jobs" | bc -l`" >> $output
#info "Processing of gate jobs is finished."

echo "########################################################" >> $output

#info of Merge jobs

echo "*************Computational cost of Merge jobs***********" >> $output
echo "--------------------------------------------------------" >> $output
echo "|JOB ID,NBR EVENTS,EXE_TIME,BOGOMIPS,COMPUTATIONAL COST|" >> $output
echo "--------------------------------------------------------" >> $output
for file in ${log_dir}/${workflow_dir}/out/merge*.sh.out; do
    chk_file=$(awk '/] Total running time:/''{print}' $file)
    if [ "$chk_file" != "" ]
    then
        merge_exe_time=$(awk '/] Total running time:/''{print}' $file | awk '{print $(NF-1)}')
        merge_num_event=$(awk '/totalEvents=/''{print}' $file | awk -F'=' '{print $2}')
        cpu_bogomips=$(awk '/bogomips/ {print $NF}' $file | uniq | awk 'NR==1') 
        merge_job_id=$(awk '/^JOBID/''{print}' $file | awk -F'=' '{print $2}')
        merge_computational_cost=`echo "$cpu_bogomips * $merge_exe_time" | bc -l`
        merge_total_comput_cost=`echo "$merge_total_comput_cost + $merge_computational_cost" | bc -l`
        merge_total_event=`echo "$merge_total_event + $merge_num_event" | bc`
        echo "$merge_job_id,$merge_num_event,$merge_exe_time,$cpu_bogomips,$merge_computational_cost" >> $output
    fi
done
echo "--------------------------------------------------------" >> $output


echo "Merge total events = $merge_total_event" >> $output
echo "Merge average Computational cost : `echo "$merge_total_comput_cost / $num_merge_jobs" | bc -l`" >> $output

echo "--------------------------------------------------------" >> $output
echo "Number of total events : `echo "$merge_total_event + $gate_total_event " | bc`" >> $output
echo "Average of Computational cost : `echo "$merge_total_comput_cost + $gate_total_comput_cost / 2" | bc`" >> $output
#info "Processing of Merge jobs is finished."


echo "*************Communicational size Gate job (in byte)***********" >> $output
echo "Communication size of Gate (Download): `tail -n +2 $transfer_info | awk -F',' '{if ($NF == "1"){total_size+=$3;};}END{print total_size }'`" >> $output
echo "Communication size of Gate (Upload): `tail -n +2 $transfer_info | awk -F',' '{if ($NF == "0"){total_size+=$3;};}END{print total_size }'`" >> $output
