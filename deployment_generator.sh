#! /bin/bash -u
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER, Mohammad Mahdi BAZM (2015)                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

# Input parameter is the name of folder that contains all of log files
# and database for a workflow
workflow=${1:? "Name of workflow folder is mandatory!!"}
initial=${2:-"standalone"}

LFC_catalog="LfcCatalog_$workflow.csv"

if [ $initial == "initial" ]
then 
    file_transfer="file_transfer.csv"
    db_dump="db_dump.csv"
    config="configParser.txt"
    output_dir="."
else
    file_transfer="csv_files/file_transfer.csv"
    db_dump="csv_files/db_dump.csv"
    config="../../scripts/configParser.txt"
    output_dir="simgrid_files"
    echo -e [`date +"%D %T"`] "Deployment file regeneration" >> README
fi

# Default Storage Element
defSE=$(awk -F'=' '/defSE/ {print $2}' $config)

#Get the path of logs folder that contain all workflow folders.
log_dir=$(awk -F'=' '/log_folder/ {print $2}' $config)

if [ $initial != "initial" ]
then 
    log_dir=../$log_dir
fi

output_file="$output_dir/deployment_${workflow}.xml"

header="<?xml version='1.0'?>"\
"\n<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">"\
"\n<platform version=\"3\">\n"\
"\t<process host=\"vip.creatis.insa-lyon.fr\" function=\"VIPServer\"/>\n"

echo -e $header > $output_file

######################## add SEs to deployment file ############################

sed "1d" $file_transfer | grep -v $defSE | \
    awk -F ',' '{printf "\t<process host=\"";
                 if ($NF=="2") printf $3; else {printf $4};
                 print "\" function=\"SE\"/>"}' | sort | uniq >> $output_file

echo -e '\n\t<process host="'$defSE'" function="DefaultSE"/>' >> $output_file

################################################################################

echo -e "\t<process host=\"lfc-biomed.in2p3.fr\" function=\"DefaultLFC\">\n"\
"\t\t<argument value=\"simgrid_files/$LFC_catalog\"/>\n"\
"\t</process>\n" >> $output_file 

################################################################################

join \
    <(grep "1$" $file_transfer | awk -F',' '{if ($5>20) print $2" "$5}' | sort)\
    <(sed "1d" $db_dump) | awk \
    '{printf "\t<process host=\""$4"\" function=\""; 
      if ($3 == "merge.sh") 
        print "Merge\">"; 
      else 
        {print "Gate\" start_time=\""$8"\">"};
      print "\t\t<argument value=\""$1"\"/>";
      print "\t\t<argument value=\""$11"\"/>";
      print "\t\t<argument value=\""$2"\"/>";
      print "\t</process>"}' >> $output_file 

echo "</platform>" >> $output_file

