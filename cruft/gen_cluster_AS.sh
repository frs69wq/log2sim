#!/bin/bash -u

##################################################################
# This script generates a deployment file in format XML for using#
# in Simgrid. Information will be retrieved from DataBase of     #
# Workflow.                                                      #
# @author:F.suter                                                #
# Company: CC-IN2P3 & CREATIS laboratory                         #
##################################################################

machine_info=$1
transfer_info=$2
tag=$3
defSE=${4:? Default SE must be passed as argument}
output_xml=$5

master="vip.creatis.insa-lyon.fr" #Default Master
lfc="lfc-biomed.in2p3.fr" #Default LFC


echo "<?xml version='1.0'?>" > $output_xml
echo "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">" >> $output_xml
echo "<platform version=\"3\">" >> $output_xml

echo " <AS id=\"AS_"$tag"\" routing=\"Cluster\">" >> $output_xml
echo -e "\t<router id=\""$tag"_router\"/>\n" >> $output_xml


echo '<host id="vip.creatis.insa-lyon.fr" power="10Gf" core="4"/>' >> $output_xml #Default Master
echo '<host id="lfc-biomed.in2p3.fr" power="10Gf" core="4"/>' >> $output_xml #Default LFC

#Check if default SE is in file_transfer.csv file, if yes:don't add defSE to platform.xml otherwise add it to platform.xml

if grep -q $defSE $transfer_info
then
  : #do nothing
else
  echo '<host id="ccsrm02.in2p3.fr" power="10Gf" core="4"/>' >> $output_xml #Default SE 
fi

# tail -n +2 $machine_info => remove the header line
# sort -t\, -k1rn => sort entries in files by descending (-r) timestamps 
#                    first (-k1) numeric (-n) field when separated by ',' (-t\,)
# awk -F, '!a[$2]++'` => separate fields by , (-F,) 
#                     => keep only the first occurrence of second field (machine name)
# explanation in ex9 on http://www.theunixschool.com/2012/06/awk-10-examples-to-group-data-in-csv-or.html

for line in `tail -n +2 $machine_info | sort -t\, -k1rn | awk -F, '!a[$2]++'`  
do   
    echo -n $line | awk -F',' '{print \
        "\t<host id=\""$2"\" power=\""$4"\" core=\""$3"\">\n" \
        "\t\t <prop id=\"closeSE\" value=\"" $7 "\"/>\n" \
        "\t</host>\n"\
        "\t<link id=\""$2"_link\" bandwidth=\"1Gbps\" latency=\"1ns\"/>\n" \
        "\t<host_link id=\""$2"\" up=\""$2"_link\" down=\""$2"_link\"/>\n"}' >>$output_xml 
done 

       # "\t<link id=\""$2"_link\" bandwidth=\""$5"\" latency=\"1ns\"/>\n" \
tail -n +2 $transfer_info | \
awk -F',' '{gsub("ms","",$4); \
 if ($NF == "0") \
   {bw[$2] +=$3/$4;count[$2]++;} \
 else \
   {bw[$1] +=$3/$4 count[$1]++;}} \ 
 END {\
   for (id in bw) 
      {print id" "bw[id]/count[id]}
 }' | awk '{ \
 print "\t<host id=\"" $1 "\" power=\"100Gf\"/>\n" \
   "\t<link id=\"" $1 "_UP\" bandwidth=\"" $2 "kBps\" latency=\"1ns\"/>\n" \
   "\t<link id=\"" $1 "_DOWN\" bandwidth=\"" $2 "kBps\" latency=\"1ns\"/>\n" \
   "\t<host_link id=\"" $1 "\" up=\"" $1 "_UP\" down=\"" $1 "_DOWN\"/>\n" 
}' >>$output_xml

#echo -e "\t<backbone id=\""$tag"_backbone\" bandwidth=\"100GBps\" latency=\"1ns\" sharing_policy=\"FATPIPE\"/>"  >>$output_xml 
echo -e "\t<backbone id=\""$tag"_backbone\" bandwidth=\"100GBps\" latency=\"1ns\" />"  >>$output_xml 
echo " </AS>" >> $output_xml
echo "</platform>" >> $output_xml
