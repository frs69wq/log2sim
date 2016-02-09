#!/bin/bash -u
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER, Mohammad Mahdi BAZM (2015)                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################
workflow_dir=${1:? "Name of workflow folder is mandatory!!"}
initial=${2:-"standalone"}

if [ $initial == "initial" ]
then 
    file_transfer="file_transfer.csv"
    worker_nodes="worker_nodes.csv"
    output_dir="."
else
    file_transfer="csv_files/file_transfer.csv"
    worker_nodes="csv_files/worker_nodes.csv"
    output_dir="simgrid_files"
    echo -e [`date +"%D %T"`] "Mock platform regeneration" >> README
fi

output_xml="$output_dir/mock_platform_"$workflow_dir".xml"

master="vip.creatis.insa-lyon.fr" # VIP Server
lfc="lfc-biomed.in2p3.fr" # Default LFC
defSE="ccsrm02.in2p3.fr" # Default SE

routing="Cluster"

header="<?xml version='1.0'?>\n<!DOCTYPE platform SYSTEM 
\"http://simgrid.gforge.inria.fr/simgrid.dtd\">\n<platform version=\"3\">\n"

as_tag="<AS id=\"AS_"${workflow_dir}"\" routing=\""$routing"\">\n"

# VIP Server
server="\t<host id=\""$master"\" power=\"5Gf\" core=\"48\"/>\n
\t<link id=\""$master"_link\" bandwidth=\"1Gbps\" latency=\"100us\" sharing_policy=\"FULLDUPLEX\"/>\n
\t<host_link id=\""$master"\" up=\""$master"_link_UP\" 
down=\""$master"_link_DOWN\"/>\n\n"

# Default LFC
default_lfc="\t<host id=\""$lfc"\" power=\"5Gf\" core=\"48\"/>\n
\t<link id=\""$lfc"_link\" bandwidth=\"1Gbps\" latency=\"100us\" sharing_policy=\"FULLDUPLEX\"/>\n
\t<host_link id=\""$lfc"\" up=\""$lfc"_link_UP\" down=\""$lfc"_link_DOWN\"/>\n" 

# Check if default SE is in file_transfer.csv file.
# If not, add it to the platform file

if ! grep -q $defSE $file_transfer
then
    default_se="\t<host id=\"ccsrm02.in2p3.fr\" power=\"5Gf\" core=\"48\"/>\n
\t<link id=\"ccsrm02.in2p3.fr_link\" bandwidth=\"1Gbps\" latency=\"100us\" sharing_policy=\"FULLDUPLEX\"/>\n
\t<host_link id=\"ccsrm02.in2p3.fr\" up=\"ccsrm02.in2p3.fr_link_UP\"
 down=\"ccsrm02.in2p3.fr_link_DOWN\"/>\n"
else
  default_se=""
fi

echo -e $header"  "$as_tag$server $default_lfc $default_se" 
<!-- worker nodes -->" > $output_xml 

# tail -n +2 $machine_info => remove the header line
# sort -t\, -k1rn => sort entries in files by descending (-r) timestamps 
#                    first (-k1) numeric (-n) field separated by ',' (-t\,)
# awk -F, '!a[$2]++'` => separate fields by , (-F,) 
#                     => keep only the first occurrence of second field (name)
# explanation in ex9 on: 
# www.theunixschool.com/2012/06/awk-10-examples-to-group-data-in-csv-or.html

sed "1d" $worker_nodes | sort -t\, -k1rn | awk -F, '!a[$2]++' | while read line  
do   
    echo -e $line | awk -F',' '{print \
        "\t<host id=\""$2"\" power=\""$4"\" core=\""$3"\">\n" \
        "\t\t <prop id=\"closeSE\" value=\"" $NF "\"/>\n" \
        "\t</host>\n"\
        "\t<link id=\""$2"_link\" bandwidth=\"1Gbps\" latency=\"100us\" sharing_policy=\"FULLDUPLEX\"/>\n" \
        "\t<host_link id=\""$2"\" up=\""$2"_link_UP\" down=\""$2"_link_DOWN\"/>\n"}' >> $output_xml
done 

echo -e "<!-- storage elements -->" >> $output_xml

sed "1d" $file_transfer | \
  awk -F',' '{if ($5 > "0") { \
       bw=$5/($6-990); \
       if (($NF == "1" && $5 > "20") || ($NF=="0")) {\
         if (! ($4 in se)) {se[$4];\
           print "\t<host id=\"" $4 "\" power=\"5Gf\" core=\"48\"/>\n" \
                 "\t<link id=\"" $4 "_link\" bandwidth=\"1Gbps\"\
 latency=\"500us\" sharing_policy=\"FULLDUPLEX\"/>\n" \
                 "\t<host_link id=\"" $4 "\" up=\"" $4 "_link_UP\"\
 down=\"" $4 "_link_DOWN\"/>\n" \
         } \
       } else {\
         if ($NF == "2") {\
         if (!($3 in se)){se[$3];\
           print "\t<host id=\"" $3 "\" power=\"5Gf\"/ core=\"48\">\n" \
                 "\t<link id=\"" $3 "_link\" bandwidth=\"1Gbps\"\
 latency=\"500us\" sharing_policy=\"FULLDUPLEX\"/>\n" \
                 "\t<host_link id=\"" $3 "\" up=\"" $3 "_link_UP\"\
 down=\"" $3 "_link_DOWN\"/>\n" \
         }\
       } \
    }}}' >> $output_xml

echo -e "<!-- AS routing -->" >> $output_xml
    
echo -e "\t<router id=\""$workflow_dir"_router\"/>\n"\
        "\t<backbone id=\""$workflow_dir"_backbone\""\
        "bandwidth=\"10Gbps\" latency=\"500us\"/>"  >>$output_xml 
    
footer="</AS>\n</platform>"
echo -e "  "$footer >> $output_xml
