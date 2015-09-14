#! /bin/bash -u

##############################################################################
# This script generates a Simgrid deployment file                            #
# @author: Mohammad Mahdi BAZM, Frédéric SUTER                               #
# Company: CC-IN2P3 & CREATIS laboratory                                     #
##############################################################################

# local function to log the steps of execution of the script
function info {
  echo -e [`date +"%D %T"`] $*
}


# Input parameter is the name of folder that contains all of log files
# and database for a workflow
workflow_dir=${1:? "Name of workflow folder is mandatory!!"}
initial=${2:-"standalone"}
if [ $initial == "initial" ]
then 
    file_transfer="file_transfer.csv"
    worker_nodes="worker_nodes.csv"
    job_times="real_times.csv"
    LFC_catalog="LfcCatalog_$workflow_dir.csv"
    config="configParser.txt"
    output_dir="."
else
    file_transfer="csv_files/file_transfer.csv"
    worker_nodes="csv_files/worker_nodes.csv"
    job_times="timings/real_times.csv"
    LFC_catalog="simgrid_files/LfcCatalog_$workflow_dir.csv"
    config="../../scripts/configParser.txt"
    output_dir="simgrid_files"
    info "Deployment file regeneration" >> README
fi

# Default Master
master="vip.creatis.insa-lyon.fr" 
# Default LFC
lfc="lfc-biomed.in2p3.fr" 
# Default Storage Element
defSE=$(awk -F'=' '/defSE/ {print $2}' $config)

#Get the path of logs folder that contain all workflow folders.
log_dir=$(awk -F'=' '/log_folder/ {print $2}' $config)

# Get database driver from config file
db_driver=$(awk -F'=' '/db_driver/ {print $2}' $config)

if [ $initial != "initial" ]
then 
    log_dir=../$log_dir
    db_driver=../$db_driver
fi

# Get the name of the database
db_name=$(basename ${log_dir}/${workflow_dir}/db/*.h2.db .h2.db)

####### DateBase Operations 
#sql_get_jobs_info="SELECT id,DATEDIFF('SECOND',QUEUED,download) as latency,DATEDIFF('SECOND',DOWNLOAD,END_E) as ttl,status,node_name,simulation_id,DATEDIFF('SECOND',queued,download)+DATEDIFF('SECOND',download,end_e) as latency_ttl, date \
# FROM (SELECT id,CASE WHEN download is null THEN queued ELSE download END as download,queued,end_e,CASE status WHEN 'COMPLETED' THEN 0 WHEN 'CANCELLED' THEN 1 WHEN 'ERROR' THEN 2 WHEN 'STALLED' THEN 3  ELSE 4 END as status,node_name,SIMULATION_ID,SUBSTR(parsedatetime(CREATION,'yyyy-MM-dd'),0,10) as date FROM JOBS WHERE STATUS='COMPLETED')"

sql_get_jobs_info="SELECT command, node_name, \
DATEDIFF('SECOND',(SELECT MIN(QUEUED) FROM JOBS), DOWNLOAD) as START_TIME, \
DATEDIFF('SECOND',DOWNLOAD,END_E) as TTL,\
id,\
CASE status WHEN 'COMPLETED' THEN 0 WHEN 'CANCELLED' THEN 1 WHEN 'ERROR' THEN 2 WHEN 'STALLED' THEN 3  ELSE 4 END as status,\
DATEDIFF('SECOND',QUEUED, DOWNLOAD) as OLD_START_TIME \
FROM JOBS "

#info "\tSQL query to execute is: ${sql_get_jobs_info}."
#info "\tSending SQL query to DB..."

# Send SQL query to H2 for execution and write select reslut to the temporary text file.

java -cp ${db_driver} org.h2.tools.Shell -url "jdbc:h2:${log_dir}/${workflow_dir}/db/$db_name" -user gasw -password gasw -sql "$sql_get_jobs_info" > sql_select_result.txt || info "Sql query execution failed."

output_file="$output_dir/deployment_${workflow_dir}.xml"

####### File processing #######
# Delete first line of text file (header contains title of columns)
sed '1d' sql_select_result.txt > tmp.txt
# Delete last line of the text file to remove(it contains number of retrieved rows)
sed '$d' tmp.txt > sql_select_result.txt  


echo "<?xml version='1.0'?>" > $output_file
echo "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">" >> $output_file 
echo -e "<platform version=\"3\">\n" >> $output_file

echo -e '\t<process host="'$master'" function="VIPServer"/>\n' >> $output_file

#################################### add SEs to deployment file ####################################

array_SE_WN_uniq=($(tail -n +2 $file_transfer | awk -F ',' '{print $3"\n"$4}' | sort -u | uniq))

# check if element is in Worker nodes list
for element in "${array_SE_WN_uniq[@]}"
do
    if ! $(awk -F',' '{print $2}' $worker_nodes | grep -q $element) && [ "${element}" != "$defSE" ]; 
    then
	$(echo -e "\t<process host=\"$element\" function=\"SE\"/>" >> $output_file)
    fi
done

####################################################################################################

echo -e '\n\t<process host="'$defSE'" function="DefaultSE"/>' >> $output_file
echo -e '\t<process host="'$lfc'" function="DefaultLFC">' >> $output_file
echo -e '\t\t<argument value="simgrid_files/'$LFC_catalog'"/>\n\t</process>\n' >> $output_file 

while read line  
do 
    # Ignore failed jobs for now
    if [ $(echo $line | awk '{print $11}') == "0" ]
    then
	echo $line | awk '{printf "\t<process host=\""$3"\" function=\""; 
                           if ($1 == "merge.sh") print "Merge\">"; 
                           else {print "Gate\" start_time=\""$5"\">"}}' >> $output_file 
	# ; else print " kill_time=\""$5+$7"\">"}}' >> $output_file 
	wn_name=$(echo $line | awk '{print $3}')
	jobid=$(echo $line | awk '{print $9}')
	echo -e "\t\t<argument value=\""$jobid"\"/>" >> $output_file 
	awk -F',' '/'${jobid}'/ {print "\t\t<argument value=\"" $9 "\"/>"}' $job_times >> $output_file
	# the $4>20 is to discard the upload of the number of particules to 
	# merge by the Merge job (typically of size=11)
	awk -F',' '/'${jobid}'/ {if ($NF == "1" && $5 >20) 
                                 print "\t\t<argument value=\"" $5 "\"/>\n"}' $file_transfer >> $output_file
	echo -e '\t</process>\n' >> $output_file
    fi
done < sql_select_result.txt
   
echo "</platform>" >> $output_file

#Delete temporary text files.
rm -f sql_select_result.txt 
rm -f tmp.txt

