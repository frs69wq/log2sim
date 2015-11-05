#!/bin/bash -u
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER, Mohammad Mahdi BAZM (2015)                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################
# local function to log the steps of execution of the script
function info {
  echo -e [`date +"%D %T"`] "\t\t"$*
}
cd $(dirname $0)

# NB:Worker Node (WN) = machine in this file

input_log=${1:? please give log file name}
file_info=${2:-input_files_info.csv}
machine_info="worker_nodes.csv"
transfer_info="file_transfer.csv"
total_transfer_times="real_times.csv"

# checking if the file is empty (Job did not complete successfully)
chk_file=$(awk '/] Total running time:/''{print}' $input_log)
if [ "$chk_file" == "" ]
then
 exit
fi

# Get the job id as known by VIP
job_id=$(awk -F'=' '/^JOBID=/''{print $2}' $input_log)

################################################################################
###            Extraction of information related to worker nodes             ###
################################################################################

#testing the existence of the file machine_info
if [ ! -f "$machine_info" ]
then
    info "File $machine_info does not exist. Create it."
    echo "Timestamp,Name,Core,MIPS,NetSpeed,SiteName,Country,CloseSE" > $machine_info
fi
#information about each column in the file machine_info
#TIMESTAMP:
#NAME: name of worker element on grid
#CORE: number of CPU core 
#MIPS: CPU bogomips
#NETSPEED: speed of network interface
#SITE_NAME: grid site to which the worker node belongs
#COUNTRY: country where the machine is located. for example:nl->Netherlands
#CLOSE_SE: the preferred Storage Element where results are uploaded

#optional fields (not used now)
#CACHE:CPU cache
#OS: Operating system running on the machine
#PROTOCOL: protocol of network interface
#IP: IP address of network interface
#RAM: RAM capacity of the machine  

#get timestamp 
timestamp=$(awk '/START date/ {print $NF}' $input_log)

# Get machine name
machine_name=$(awk '/uname/{getline; print $2}' $input_log)

country=$(echo $machine_name | awk -F '.' '{print $NF}')


#get the number of CPU cores and put it in a local variable
#cpu_core_nb=$(awk '/cpu cores/ {print $NF}' $input_log | uniq)
cpu_core_nb=$(grep processor $input_log | wc -l)

#get bogomips
cpu_bogomips=$(awk '/bogomips/ {print $NF}' $input_log | uniq | awk 'NR==1' )  

#get Speed (and unit) of network interface
net_interface_speed=$(awk '/NetSpeed/ {if($3 ~ /[0-9]*[a-zA-Z]bps/) {print $3} else {print null}}' $input_log)
if [ "${net_interface_speed}" == "" ]
then 
    net_interface_speed="1000Mbps"
fi

#grid site
site=$(awk -F'=' '/^SITE_NAME/ {print $NF}' $input_log)

#close SE of Worker Node
dpm_host=$(awk -F'=' '/^DPM_HOST/ {print $NF}' $input_log)
close_SE=$(awk -F'=' '/^VO_BIOMED_DEFAULT_SE/ {print $NF}' $input_log)
if [ $close_SE == "DPM_HOST" ] || [ "${close_SE}" == "" ]
then 
    close_SE=$dpm_host
fi

full_info=$(echo "$machine_name,$cpu_core_nb,${cpu_bogomips}Mf,$net_interface_speed,$site,$country,$close_SE")

#write informations in the file: machine_info
if ! grep -q $full_info $machine_info ; then

  if grep -q $machine_name $machine_info ; then
    info "Different information for '$machine_name' exists in '$machine_info'."
    info "Create a new entry with timestamp '$timestamp'"
  fi
    echo "$timestamp,$full_info" >> $machine_info
else
     info "Similar information for '$machine_name' already exists in '$machine_info'."
fi

################################################################################
###            Extraction of information related to file transfers           ###
################################################################################
# Testing the existence of the file transfer_info
if [ ! -f "$transfer_info" ]
then
    info "File $transfer_info does not exist. Create it."
    echo "Timestamp,JobId,Source,Destination,FileSize,Time,UpDown" > $transfer_info
fi

#information about each column in the file $transfer_info
#JOBID: ID of the job involved in the transfer
#SOURCE: source of transfer 
#DESTINATION: destination of transfer
#FILESIZE: size of transferred file
#TIME: elapsed time to transfer the file 
#UpDown: type of transfer, i.e., UploadTest(0), Upload(1), Download(2), or Replication(3)

# get information about upload(test) transfers 
upload_duration=$(awk '/] UploadCommand=lcg-cr/' $input_log | \
    awk -F"Source=" '{gsub("="," ",$2); print $2}' | \
    awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); gsub("ms","",$7);printf '$timestamp'","'$job_id' ",'$machine_name',"$3","$5","$7","; if ($5==12) {print "0"} else {print "1"}}' | tee -a $transfer_info | awk -F',' '{total_time+=$6;}END{print total_time/1000}')

# get information about download transfers 
download_duration=$(awk '/DownloadCommand=lcg-cp.*ms$/' $input_log | awk -F"Source=" '{print $2}'| \
    awk -F"Destination=" '{count=split($1,a," "); gsub("="," ",$2);gsub("Size"," ",$2);gsub("Time"," ",$2); print a[count]" "$2}' | \
    awk -F' ' '{ gsub("ms","",$4);print '$timestamp'","'$job_id' "," $1",'$machine_name',"$3","$4  ",2"}' |\
tee -a $transfer_info | awk -F',' '{total_time+=$6;}END{print total_time/1000}')

# get information about replication transfers
awk '/] UploadCommand=lcg-rep/' $input_log | awk -F"Source=" '{print $2}'| \
    awk -F"Destination=" '{count=split($1,a," "); gsub("="," ",$2); print a[count]" "$2}' | \
    awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); print '$timestamp'","'$job_id' "," $1 ","$3","$5","$7",3"}' >> $transfer_info 

################################################################################
###             Extraction of information related to input files             ###
################################################################################

# Extract information related to input files
# Row format:
# Name of file,size of file, SE1:SE2:SE3:...:SEn
# Example: gate.sh.tar.gz,73043,wn-206-08-01-03-a.cr.cnaf.infn.it:wn1205291.tier2.hep.manchester.uk 

if [ ! -f "$file_info" ]
then
    info "File $file_info does not exist. Create it."
    touch $file_info
fi

array_file_info=($(awk '/] lcg-cp -v --connect-timeout/{nr[NR]; nr[NR+2]}; NR in nr' $input_log | \
    awk -F'/' '{print $NF}' | awk 'NR%2{printf $0" ";next;}1' | \
    awk -F'/' '{gsub("="," ",$0); gsub(/\[.*.\]/,"",$0);print}' | \
    awk '{printf $1","$9; if ($2 == "DownloadCommand") print ","$5; else print ",'$close_SE'"}'))

for fl in "${array_file_info[@]}" 
do
  filename=$(echo $fl | awk -F',' '{gsub("dsarrut_","",$1); print $1}')
  se=$(echo $fl | awk -F',' '{print $3}')
  filesize=$(echo $fl | awk -F',' '{print $2}')
  if ! grep -q "$filename" $file_info
  then
    if [[ $filesize =~ ^[0-9]+$ ]] && [[ $filename != "DownloadCommand" ]]
    then
       echo "inputs/$filename,$filesize,$se" >> $file_info;
    fi
  else
       { rm -f $file_info && awk -F',' -v n="$filename" -v s="$se" '{if (match($1,n) && !match($3,s)){sub($3,$3":"s,$3);gsub(" ",",",$0)}}1' > $file_info;} < $file_info ;
  fi
done

################################################################################
###               logging cumulated data transfer time per job               ###
################################################################################

if [ ! -f "$total_transfer_times" ]
then
    info "File $total_transfer_times does not exist. Create it."
    echo -e "JobId,DownloadDuration_File,UploadDuration_File" \
	> $total_transfer_times
fi

echo -e "$job_id,$download_duration,$upload_duration" >> $total_transfer_times 
