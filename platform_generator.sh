#!/bin/bash -u

##############################################################################
# This script generates platform files in the SimGrid XML. Information is    #
# retrieved from CSV files extracted from the log files.                     #
# @author: Mohammad Mahdi BAZM, Frédéric SUTER                               #
# Company: CC-IN2P3 & CREATIS laboratory                                     #
##############################################################################

workflow_dir=${1:? "Name of workflow folder is mandatory!!"}
initial=${2:-"standalone"}

if [ $initial == "initial" ]
then 
    file_transfer="file_transfer.csv"
    worker_nodes="worker_nodes.csv"
    se_bandwidth="se_bandwidth.csv"
    config="configParser.txt"
    output_dir="."
else
    file_transfer="csv_files/file_transfer.csv"
    worker_nodes="csv_files/worker_nodes.csv"
    se_bandwidth="csv_files/se_bandwidth.csv"
    config="../../scripts/configParser.txt"
    output_dir="simgrid_files"
    echo -e [`date +"%D %T"`] "Platform files regeneration" >> README
fi

max_sym="$output_dir/platform_"$workflow_dir"_max_symmetric.xml"
avg_sym="$output_dir/platform_"$workflow_dir"_avg_symmetric.xml"
max_asym="$output_dir/platform_"$workflow_dir"_max_asymmetric.xml"
avg_asym="$output_dir/platform_"$workflow_dir"_avg_asymmetric.xml"

master="vip.creatis.insa-lyon.fr" # VIP Server
lfc="lfc-biomed.in2p3.fr" # Default LFC
defSE=$(awk -F'=' '/defSE/ {print $2}' $config) # Default SE

routing="Cluster"

header="<?xml version='1.0'?>\n<!DOCTYPE platform SYSTEM 
\"http://simgrid.gforge.inria.fr/simgrid.dtd\">\n<platform version=\"3\">\n"

as_tag="<AS id=\"AS_"${workflow_dir}"\" routing=\""$routing"\">\n"

# VIP Server
server="\t<host id=\""$master"\" power=\"100Gf\" core=\"4\"/>\n
\t<link id=\""$master"_link\" bandwidth=\"10Gbps\" latency=\"1ns\"/>\n
\t<host_link id=\""$master"\" up=\""$master"_link\" 
down=\""$master"_link\"/>\n\n"

# Default LFC
default_lfc="\t<host id=\""$lfc"\" power=\"100Gf\" core=\"4\"/>\n
\t<link id=\""$lfc"_link\" bandwidth=\"10Gbps\" latency=\"1ns\"/>\n
\t<host_link id=\""$lfc"\" up=\""$lfc"_link\" down=\""$lfc"_link\"/>\n" 

# Check if default SE is in file_transfer.csv file.
# If not, add it to the platform file

if ! grep -q $defSE $file_transfer
then
    default_se="\t<host id=\"ccsrm02.in2p3.fr\" power=\"100Gf\" core=\"4\"/>\n
\t<link id=\"ccsrm02.in2p3.fr_link\" bandwidth=\"10Gbps\" latency=\"1ns\"/>\n
\t<host_link id=\"ccsrm02.in2p3.fr\" up=\"ccsrm02.in2p3.fr_link\"
 down=\"ccsrm02.in2p3.fr_link\"/>\n"
else
  default_se=""
fi

for output_xml in $max_sym $max_asym $avg_sym $avg_asym
do
    echo -e $header"  "$as_tag$server $default_lfc $default_se"
<!-- worker nodes -->" > $output_xml 
done

# tail -n +2 $machine_info => remove the header line
# sort -t\, -k1rn => sort entries in files by descending (-r) timestamps 
#                    first (-k1) numeric (-n) field separated by ',' (-t\,)
# awk -F, '!a[$2]++'` => separate fields by , (-F,) 
#                     => keep only the first occurrence of second field (name)
# explanation in ex9 on: 
# www.theunixschool.com/2012/06/awk-10-examples-to-group-data-in-csv-or.html

for line in `tail -n +2 $worker_nodes | sort -t\, -k1rn | awk -F, '!a[$2]++'`  
do   
    worker=$(echo $line | awk -F',' '{print \
        "\\t<host id=\""$2"\" power=\""$4"\" core=\""$3"\">\\n" \
        "\\t\\t <prop id=\"closeSE\" value=\"" $7 "\"/>\\n" \
        "\\t</host>\\n"\
        "\\t<link id=\""$2"_link\" bandwidth=\""$5"\" latency=\"1ns\"/>\\n" \
        "\\t<host_link id=\""$2"\" up=\""$2"_link\" down=\""$2"_link\"/>\\n"}')
    for output_xml in $max_sym $max_asym $avg_sym $avg_asym
    do
	echo -e $worker >>$output_xml 
    done
done 

for output_xml in $max_sym $max_asym $avg_sym $avg_asym
do
    echo -e "<!-- storage elements -->" >> $output_xml
done

for line in `tail -n +2 $se_bandwidth`
do
   se=$(echo $line | 
       awk -F',' '{print \
       "\\t<host id=\"" $1 "\" power=\"100Gf\"/>\\n";}')
   for output_xml in $max_sym $max_asym $avg_sym $avg_asym
   do   
       case $output_xml in
	   $avg_sym )
	       links=$(echo $line | awk -F',' '{\
                  print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $2 "kBps\"\
 latency=\"1ns\"/>\\n" \
                        "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link\"\
 down=\"" $1 "_link\"/>\\n"\
                  }');;
	   $max_sym )
	       links=$(echo $line | awk -F',' '{\
                  print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $3 "kBps\" latency=\"1ns\"/>\\n" \
                        "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link\" down=\"" $1 "_link\"/>\\n"\
                  }');;
	   $avg_asym )
	       links=$(echo $line | awk -F',' '{if($4 == "0" || $6 == "0"){\
                   print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $2 "kBps\"\
 latency=\"1ns\"/>\\n" \
                         "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link\"\
 down=\"" $1 "_link\"/>\\n"\
                   } else { \
                     print "\\t<link id=\"" $1 "_UP\" bandwidth=\"" $6 "kBps\"\
 latency=\"1ns\"/>\\n" \
                           "\\t<link id=\"" $1 "_DOWN\" bandwidth=\"" $4 "kBps\" latency=\"1ns\"/>\\n" \
                           "\\t<host_link id=\"" $1 "\" up=\"" $1 "_UP\"\
 down=\"" $1 "_DOWN\"/>\\n" 
                   }}') ;;
	   $max_asym )
	       links=$(echo $line | awk -F',' '{if($4 == "0" || $6 == "0"){\
                   print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $3 "kBps\"\
 latency=\"1ns\"/>\\n" \
                         "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link\"\
 down=\"" $1 "_link\"/>\\n"\
                   } else { \
                     print "\\t<link id=\"" $1 "_UP\" bandwidth=\"" $7 "kBps\"\
 latency=\"1ns\"/>\\n" \
                           "\\t<link id=\"" $1 "_DOWN\" bandwidth=\"" $5 "\
kBps\" latency=\"1ns\"/>\\n" \
                           "\\t<host_link id=\"" $1 "\" up=\"" $1 "_UP\"\
 down=\"" $1 "_DOWN\"/>\\n" 
                   }}');;
       esac
       echo -e $se$links >>$output_xml 
   done
done

for output_xml in $max_sym $max_asym $avg_sym $avg_asym
do
    echo -e "<!-- AS routing -->" >> $output_xml
    
    echo -e "\t<router id=\""$workflow_dir"_router\"/>\n"\
            "\t<backbone id=\""$workflow_dir"_backbone\""\
            "bandwidth=\"100GBps\" latency=\"1ns\"/>"  >>$output_xml 
    
    footer="</AS>\n</platform>"
    echo -e "  "$footer >> $output_xml
done   
