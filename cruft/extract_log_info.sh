#!/bin/bash -u
cd $(dirname $0)
#NB:Worker node(WN)= machine in this file

# command line arguments
# TODO: check numbers of arguments + add defaults names for outputs
# Example: "./extract_data.sh execution_trace.out" should be valid  

input_log=${1:? please give log file name}
machine_info=${2:-worker_nodes.csv}
transfer_info=${3:-file_trnasfer.csv}

function info {
  local DATE=`date`
  echo [ $DATE ] $*
}

info $input_log # input file that contains log of execution a job
info $machine_info # output file CSV, that contains information about the machines of grid
info $transfer_info # output file CSV, that contains information about file transfers between two elements

#checking if the file is empty(Job Completed successfully)
chk_file=$(awk '/] Total running time:/''{print}' $input_log)
if [ "$chk_file" == "" ]
then
 exit
else 
  :
fi



################################################################################
###            Extraction of information relative to worker nodes            ###
################################################################################

#testing the existence of the file machine_info
if [ ! -f "$machine_info" ]
then
    info "File $machine_info does not exist"
    info "Creating $machine_info ....."
    echo "TIMESTAMP,NAME,CORE,MIPS,NETSPEED,COUNTRY,DEFSE" > $machine_info
fi
#information about each column in the file machine_info
#TIMESTAMP:
#NAME: name of worker element on grid
#CORE: number of CPU core 
#MIPS: CPU bogomips
#NETSPEED: speed of network interface 
#COUNTRY: country where the machine is located. for example:nl->Netherlands

#optional fields (not used now)
#CACHE:CPU cache
#OS: Operating system running on the machine
#PROTOCOL: protocol of network interface
#IP: IP address of network interface
#RAM: RAM capacity of the machine  


# Testing the existence of the file transfer_info
if [ ! -f "$transfer_info" ]
then
    info "File $transfer_info does not exist"
    info "Creating $transfer_info ....."
    echo "SOURCE,DESTINATION,FILESIZE,TIME,UpDown" > $transfer_info
fi
#information about each column in the file $transfer_info
#SOURCE: source of transfer 
#DESTINATION: destination of transfer
#FILESIZE: the of transferd file between two elements on grid
#TIME: elapsed time to transfer the file between two elements on grid
#UpDown: type of transfering Download or Upload


#get timestamp 
timestamp=$(awk '/START date/ {print $NF}' $input_log)

#get name of machine
machine_name=$(awk '/uname/{getline; print}' $input_log | awk '{print $2;}')

#get the number of CPU cores and put it in a local variable
cpu_core_nb=$(awk '/cpu cores/ {print $NF}' $input_log | uniq)

#get bogomips
cpu_bogomips=$(awk '/bogomips/ {print $NF}' $input_log | uniq | awk 'NR==1' )  

#get Speed (and unit) of network interface
net_interface_speed=$(awk '/NetSpeed/ {if($3 ~ /[0-9]*[a-zA-Z]bps/) {print $3} else {print null}}' $input_log)
    
#get country of machine
machine_country=$(echo $machine_name | awk -F '.' '{print $NF}')

#Default SE of Worker Node
default_SE=$(awk '/VO_BIOMED_DEFAULT_SE=/' $input_log | awk -F'=' '{print $NF}')

if [ "${default_SE}" == "" ]
then 
    default_SE="null"
fi
if [ "${net_interface_speed}" == "" ]
then 
    net_interface_speed="1000Mbps"
fi


full_info=$(echo "$machine_name,$cpu_core_nb,${cpu_bogomips}Mf,$net_interface_speed,$machine_country,$default_SE")

#write informations in the file: machine_info
if ! grep -q $full_info $machine_info ; then

  if grep -q $machine_name $machine_info ; then
    info "Different information for '$machine_name'"
    info "exists in '$machine_info'."
    info "Create a new entry with timestamp '$timestamp'"
  fi
    echo "$timestamp,$full_info" >> $machine_info
else
     info "Similar information for '$machine_name' already exists in '$machine_info'."
fi


#### POTENTIAL EXTRA INFORMATION ####

# get OS of machine (not used now)
# machine_os=$(awk '/uname/{getline; print}' $input_log | awk '{print $1;}')

#get ip of network card (not used now)
#net_interface_ip=$(grep -o 'inet\s[a-zA-Z]*\s*:[0-9]*'.[0-9]*.[0-9]*.[0-9]* $input_log |  uniq)
#net_interface_ip=${net_interface_ip#*:} # take ip address
    
#get protocol of network interface
# net_interface_protocol="" #ifconfig eth0 | awk '/Link encap:/{print}' | awk '{print $3}' | awk -F":" '{print $2}'
    
# get the frequency of CPU and put it in a local variable (bogomips are better)
# cpu_frequency=$(grep -o '[0-9]*.[0-9]*GHz' $input_log | uniq)
    
#get the size of CPU cache (not used now)
#cpu_cache=$(grep -o 'cache size\s:\s[0-9]*\s[a-zA-Z]*' $input_log | uniq)
#cpu_cache=${cpu_cache#*:}

#get the size of RAM memory
#RAM=$(grep -o 'MemTotal:[0-9]*\s'KB $input_log)


################################################################################
###          Extraction of information relative to file transfers            ###
################################################################################

#Two types of file transfer:Download/Upload
#If download,write 1 in the file .CSV
#If upload,write 0 in the file .CSV


# get information about upload transfers  
# awk '/] UploadCommand/' => keep only the lines that contains "UploadCommand"
# awk -F"Source=" '{gsub("="," ",$2); print $2}' 
#     => cut everything before "Source=" (-F + $2)
#     => replace '=' by spaces in what remains (gsub)
# awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); print $1","$3","$5","$7",0"}'
#     => handle the possibility to have machine name postfixed by a port number
#          => cut everything after the ':' (gsub(/:.*/,"",$1) for source
#          => cut everything after the ':' (gsub(/:.*/,"",$3) for destination
#     => format the entry as wanted: source, destination, size, time, up/down

awk '/] UploadCommand=lcg-cr/' $input_log | awk -F"Source=" '{gsub("="," ",$2); print $2}' | \
awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); print $1","$3","$5","$7",0"}' >> $transfer_info

# get information about download transfers 
awk '/] DownloadCommand=lcg-cp/' $input_log | awk -F"Source=" '{gsub("="," ",$2); print $2}' | \
awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); print $1","$3","$5","$7",1"}' >> $transfer_info
awk '/] DownloadCommand=lcg-cp/' $input_log | awk -F"Source=" '{gsub("="," ",$2); print $2}' | \
awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); print $1","$3","$5","$7",1"}' >> out.txt
echo ${input_log} >> out.txt
# get information about replication transfers
awk '/] UploadCommand=lcg-rep/' $input_log | awk -F"Source=" '{gsub("="," ",$2); print $2}' | \
awk '{gsub(/:.*/,"",$1); gsub(/:.*/,"",$3); print $1","$3","$5","$7",2"}' >> $transfer_info 


#get information about upload_test
#upload_test=$(awk '/<upload_test/,/<\/upload_test/' $input_log) #this command take lines between <upload_test></upload_test> 

# up_file_size=(`cat upload_tmp | awk -F"Size:" '{print $2}' | awk '{print $1}'`)        

# up_destination_name=(`cat upload_tmp | awk -F"Destination:" '{print $2}' | awk '{print $1}'`)

# up_transfer_time=(`cat upload_tmp | awk -F"Time:" '{print $2}'`)

# up_source_name=(`cat upload_tmp | awk -F"Source:" '{print $2}' | awk '{print $1}'`)

# up_count=${#up_destination_name[@]}
# ##write upload informations in the file: machine_info

# for i in $(seq 0 $((up_count-1))); do
#     echo "${up_source_name[${i}},${up_destination_name[${i}]},${up_file_size[${i}]},${up_transfer_time[${i}]},0" >> $transfer_info
# done

#Get information about inputs_download#

#get the size of file
# down_file_size=(`awk '/] DownloadCommand/' $input_log | awk -F"DownloadCommand:lcg-cp " '{print $2}'| awk -F"Size:" '{print $2}' | awk -F' ' '{print $1}'| awk -F":" '{print $1}'`)

# #get the time to transfer
# down_transfer_time=(`awk '/] DownloadCommand/' $input_log | awk -F"DownloadCommand:lcg-cp " '{print $2}'| awk -F"Time:" '{print $2}' | awk -F' ' '{print $1}'| awk -F":" '{print $1}'`)

# #get the name of SE
# #down_destination_name=$(awk '/<inputs_download/,/<\/inputs_download/' $input_log |awk '/is on local SE/''{print $NF}')
# down_destination_name=(`awk '/] DownloadCommand/' $input_log | awk -F"DownloadCommand:lcg-cp " '{print $2}'| awk -F"Destination:" '{print $2}' | awk -F' ' '{print $1}'| awk -F":" '{print $1}'`)

# down_source_name=(`awk '/] DownloadCommand/' $input_log | awk -F"DownloadCommand:lcg-cp " '{print $2}'| awk -F"Source:" '{print $2}' | awk -F' ' '{print $1}'| awk -F":" '{print $1}'`)

# #Get number of elements in array
# down_count=${#down_source_name[@]}
# #write download informations in the file: machine_info
# #echo $down_count
# for j in $(seq 0 $((down_count-1))); do
#     echo "${down_source_name[${j}]},${down_destination_name[${j}]},${down_file_size[${j}]},${down_transfer_time[${j}]},1" >> $transfer_info
# done

