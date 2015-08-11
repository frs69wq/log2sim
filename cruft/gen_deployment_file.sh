#! /bin/bash -u

##################################################################
# This script generates a deployment file in format XML for using#
# in Simgrid. Information will be retrieved from DataBase of     #
# Workflow.                                                      #
# @author:Mohammad Mahdi BAZM                                    #
# Company: CC-IN2P3 & CREATIS laboratory                         #
##################################################################

#Input parameter is the name of folder that contains all of log files and DB file related to a workflow
workflow_dir=${1:? "Name of workflow folder is mandatory!!"}


#Define constant varialbes
master="vip.creatis.insa-lyon.fr" #Default Master
lfc="lfc-biomed.in2p3.fr" #Default LFC
defSE="ccsrm02.in2p3.fr" #Default SE 

#local function to log execution steps of the script.
function info {
  local DATE=`date`
  echo [ $DATE ] $*
}

#Get database driver from config file
db_driver=$(awk -F'=' '/db_driver/ {print $2}' configParser.txt)
#Get the name of folder that contains log files
log_dir=$(awk -F'=' '/log_folder/ {print $2}' configParser.txt)
#Get the name of DB
db_name=$(basename ${log_dir}/${workflow_dir}/db/*.h2.db .h2.db)


info "The name of DataBase is:${db_name}"


####### DateBase Operations 
# SQL query to cache information from Database.
#sql_get_jobs_info="SELECT ID,DATEDIFF('SECOND',QUEUED,RUNNING) as latency, DATEDIFF('SECOND',RUNNING,END_E) as ttl, \
#CASE STATUS WHEN 'COMPLETED' THEN 0 WHEN 'CANCELLED' THEN 1 WHEN 'ERROR' THEN 2 WHEN 'STALLED' THEN 3  ELSE 4 END as status , \
#CASE WHEN node_name IS null THEN CONCAT('wn','-',ID,'.','dc','.','creatis','.','fr') ELSE node_name END as NODE_NAME, \
#SIMULATION_ID,DATEDIFF('SECOND',QUEUED,RUNNING)+DATEDIFF('SECOND',RUNNING,END_E) as latency_ttl,SUBSTR(parsedatetime(CREATION, 'yyyy-MM-dd'),0,10)  as date  FROM JOBS "

sql_get_jobs_info="SELECT id,DATEDIFF('SECOND',QUEUED,download) as latency,DATEDIFF('SECOND',DOWNLOAD,END_E) as ttl,status,node_name,simulation_id,DATEDIFF('SECOND',queued,download)+DATEDIFF('SECOND',download,end_e) as latency_ttl, date \
FROM (SELECT id,CASE WHEN download is null THEN queued ELSE download END as download,queued,end_e,CASE status WHEN 'COMPLETED' THEN 0 WHEN 'CANCELLED' THEN 1 WHEN 'ERROR' THEN 2 WHEN 'STALLED' THEN 3  ELSE 4 END as status,node_name,SIMULATION_ID,SUBSTR(parsedatetime(CREATION,'yyyy-MM-dd'),0,10) as date FROM JOBS WHERE NODE_NAME IS NOT null)"

info "SQL query to execute is: ${sql_get_jobs_info}."
info "Sending SQL query to DB..."

#Send SQL query to H2 for execution and write select reslut to the temporary text file.

java -cp ${db_driver} org.h2.tools.Shell -url "jdbc:h2:${log_dir}/${workflow_dir}/db/$db_name" -user gasw -password gasw -sql "$sql_get_jobs_info" > sql_select_result.txt || info "Sql query execution failed."


#generate the name of output file from SIMULATION_ID and CREATION date in DB ex: workflow-kgGfoN_2014-04-11.xml
workflow_id=$(awk 'NR==2 {print $11}' sql_select_result.txt)
workflow_date=$(awk 'NR==2 {print $15}' sql_select_result.txt)
output_file="${workflow_id}_${workflow_date}.xml"

#Delete output file(Deployment file with the same name) if already exist.
if [ -f "$output_file" ]
then 
    `rm $output_file`
    info "$output_file is already exist, File is deleted."
else
    info "$output_file is created."
fi


####### File processing #######
#Delete first line of text file(header contains title of coloumns)
sed -i '1d' sql_select_result.txt
#Delete last line of the text file to remove(it contains number of retrieved rows)
sed -i '$d' sql_select_result.txt  


echo "<?xml version='1.0'?>" > $output_file
echo "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">" >> $output_file 
echo "<platform version=\"3\">" >> $output_file
echo -e '\t<process host="'$master'" function="master"/> <!-- demaster -->' >> $output_file
echo -e '\t<process host="'$lfc'" function="lfc"/> <!-- delfc -->' >> $output_file


#Read rows of text file and write in the outfile 
#output of WHILE loop will be like:
#  <process host="griffon-85.nancy.grid5000.fr" function="slave">
#       <argument value="chicon-11.lille.grid5000.fr"/> <!-- defSE -->
#       <argument value="0"/> <!-- latency -->
#       <argument value="00"/> <!-- latency + ttl -->
#       <argument value="0"/> <!-- status -->
#  </process>

while read line  
do 
   echo $line | awk '{print "\t<process host=\""$9"\" function=\"slave\">"}' >> $output_file 
   echo -e "\t\t<argument value=\""$defSE"\"/>" >> $output_file
   echo $line | awk '{print \
   "\t\t<argument value=\""$5"\"/>\n" \
   "\t\t<argument value=\""$13"\"/>\n" \
   "\t\t<argument value=\""$7"\"/>\n" \
   "\t</process>\n"}' >> $output_file     
done < sql_select_result.txt
   

echo "</platform>" >> $output_file


#Delete temporary text file.
`rm sql_select_result.txt` 
info "Deployment file is created:$output_file"
echo ${output_file}



