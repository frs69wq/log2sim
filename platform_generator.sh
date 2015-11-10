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
    config="configParser.txt"
    output_dir="."
else
    file_transfer="csv_files/file_transfer.csv"
    worker_nodes="csv_files/worker_nodes.csv"
    config="../../scripts/configParser.txt"
    output_dir="simgrid_files"
    echo -e [`date +"%D %T"`] "Platform files regeneration" >> README
fi

max_sym="$output_dir/platform_"$workflow_dir"_max_symmetric.xml"
#avg_sym="$output_dir/platform_"$workflow_dir"_avg_symmetric.xml"
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
server="\t<host id=\""$master"\" power=\"5Gf\" core=\"4\"/>\n
\t<link id=\""$master"_link\" bandwidth=\"10Gbps\" latency=\"500us\"/>\n
\t<host_link id=\""$master"\" up=\""$master"_link\" 
down=\""$master"_link\"/>\n\n"

# Default LFC
default_lfc="\t<host id=\""$lfc"\" power=\"5Gf\" core=\"4\"/>\n
\t<link id=\""$lfc"_link\" bandwidth=\"10Gbps\" latency=\"500us\"/>\n
\t<host_link id=\""$lfc"\" up=\""$lfc"_link\" down=\""$lfc"_link\"/>\n" 

# Check if default SE is in file_transfer.csv file.
# If not, add it to the platform file

if ! grep -q $defSE $file_transfer
then
    default_se="\t<host id=\"ccsrm02.in2p3.fr\" power=\"5Gf\" core=\"4\"/>\n
\t<link id=\"ccsrm02.in2p3.fr_link\" bandwidth=\"10Gbps\" latency=\"500us\"/>\n
\t<host_link id=\"ccsrm02.in2p3.fr\" up=\"ccsrm02.in2p3.fr_link\"
 down=\"ccsrm02.in2p3.fr_link\"/>\n"
else
  default_se=""
fi

for output_xml in $max_sym $max_asym $avg_asym #$avg_sym
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

sed "1d" $worker_nodes | sort -t\, -k1rn | awk -F, '!a[$2]++' | while read line  
do   
    worker=$(echo $line | awk -F',' '{print \
        "\\t<host id=\""$2"\" power=\""$4"\" core=\""$3"\">\\n" \
        "\\t\\t <prop id=\"closeSE\" value=\"" $NF "\"/>\\n" \
        "\\t</host>\\n"\
        "\\t<link id=\""$2"_link\" bandwidth=\""$5"\" latency=\"500us\"/>\\n" \
        "\\t<host_link id=\""$2"\" up=\""$2"_link\" down=\""$2"_link\"/>\\n"}')
    for output_xml in $max_sym $max_asym $avg_asym # $avg_sym
    do
	echo -e $worker >>$output_xml 
    done
done 

for output_xml in $max_sym $max_asym $avg_asym # $avg_sym
do
    echo -e "<!-- storage elements -->" >> $output_xml
done

sed "1d" $file_transfer | \
  awk -F',' '{if ($5 > "0") { \
       bw=$5/($6-990); \
       if ($NF == "1" && $5 > "20") {\
         if (! ($4 in se)) {se[$4]}; \
         download_count[$4]+=1; \
         total_bw[$4] += bw; \
         if (bw > max_bandwidth[$4]){max_bandwidth[$4]=bw};\
         total_download_bw[$4] += bw; \
         if (bw > max_download_bandwidth[$4]){max_download_bandwidth[$4]=bw};\
       } else {\
         if ($NF == "2") {\
           if (!($2 in se)){se[$3]}; \
             upload_count[$3]+=1; \
             total_bw[$3] += bw; \
             if (bw > max_bandwidth[$3]){max_bandwidth[$3]=bw};\
             total_upload_bw[$3] += bw; \
             if (bw > max_upload_bandwidth[$3]){max_upload_bandwidth[$3]=bw};\
           }\
         } \
    }} END { \
      for (id in se) {\
         printf id","(total_bw[id]/(download_count[id]+upload_count[id]))","\
                max_bandwidth[id]",";
         if (download_count[id] > 0){\
           printf (total_download_bw[id]/download_count[id])","\
                  max_download_bandwidth[id]"," \
         } else {\
           printf "0,0,"
         } \
         if (upload_count[id] > 0){\
           print (total_upload_bw[id]/upload_count[id])","\
                  max_upload_bandwidth[id]\
         } else {\
           print "0,0"
         } \
      }\
    }' >> se_bandwidth.csv


cat se_bandwidth.csv | while read line
do
   se=$(echo $line | 
       awk -F',' '{print \
       "\\t<host id=\"" $1 "\" power=\"5Gf\"/>\\n";}')
   for output_xml in $max_sym $max_asym $avg_asym #$avg_sym
   do   
       case $output_xml in
  	   $avg_asym )
 	       links=$(echo $line | awk -F',' '{if($4 == "0" || $6 == "0"){\
                   print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $2 "kBps\"\
  latency=\"500us\" sharing_policy=\"FULLDUPLEX\"/>\\n" \
                         "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link_UP\"\
  down=\"" $1 "_link_DOWN\"/>\\n"\
                     } else { \
                   print "\\t<link id=\"" $1 "_UP\" bandwidth=\"" $6 "kBps\"\
  latency=\"500us\" />\\n" \
                            "\\t<link id=\"" $1 "_DOWN\" bandwidth=\"" $4 "kBps\"\
  latency=\"500us\" />\\n" \
                            "\\t<host_link id=\"" $1 "\" up=\"" $1 "_UP\"\
  down=\"" $1 "_DOWN\"/>\\n" 
                    }}') ;;
	   $max_sym )
	       links=$(echo $line | awk -F',' '{\
                  print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $3 "kBps\"\
 latency=\"500us\" sharing_policy=\"FULLDUPLEX\"/>\\n" \
                        "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link_UP\" \
 down=\""$1"_link_DOWN\"/>\\n"\
                  }');;
	   $max_asym )
	       links=$(echo $line | awk -F',' '{if($4 == "0" || $6 == "0"){\
                   print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $3 "kBps\"\
 latency=\"500us\" sharing_policy=\"FULLDUPLEX\"/>\\n" \
                         "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link_UP\"\
 down=\"" $1 "_link_DOWN\"/>\\n"\
                   } else { \
                     print "\\t<link id=\"" $1 "_UP\" bandwidth=\"" $7 "kBps\"\
 latency=\"500us\"/>\\n" \
                           "\\t<link id=\"" $1 "_DOWN\" bandwidth=\"" $5 "\
kBps\" latency=\"500us\"/>\\n" \
                           "\\t<host_link id=\"" $1 "\" up=\"" $1 "_UP\"\
 down=\"" $1 "_DOWN\"/>\\n" 
                   }}');;
 # 	   $avg_sym )
 # 	       links=$(echo $line | awk -F',' '{\
 #                  print "\\t<link id=\"" $1 "_link\" bandwidth=\"" $2 "kBps\"\
 # latency=\"500us\" sharing_policy=\"FATPIPE\"/>\\n" \
 #                        "\\t<host_link id=\"" $1 "\" up=\"" $1 "_link\"\
 # down=\"" $1 "_link\"/>\\n"\
 #                  }');;
       esac
       echo -e $se$links >>$output_xml 
   done
done

for output_xml in $max_sym $max_asym $avg_asym # $avg_sym
do
    echo -e "<!-- AS routing -->" >> $output_xml
    
    echo -e "\t<router id=\""$workflow_dir"_router\"/>\n"\
            "\t<backbone id=\""$workflow_dir"_backbone\""\
            "bandwidth=\"100GBps\" latency=\"1500us\"/>"  >>$output_xml 
    
    footer="</AS>\n</platform>"
    echo -e "  "$footer >> $output_xml
done   

rm -f se_bandwidth.csv
