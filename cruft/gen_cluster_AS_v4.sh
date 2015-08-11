#!/bin/bash -u

##################################################################
# This script generates a platform file in format XML for using  #
# in Simgrid. Information will be retrieved from CSV files.      #
# @author:M.bazm                                                 #
# Company: CC-IN2P3 & CREATIS laboratory                         #
##################################################################
# In this type of platform, hosts are grouped in the different ASs#
# according to the countery where hosts are situated             #
# Routing method between ASs = full                              #                               
################################################################## 

machine_info=${1:? machine_info.csv must be passed as argument}
transfer_info=${2:? transfer_info.csv must be passed as argument}
tag=$3
defSE=${4:? Default SE must be passed as argument}
output_xml=$5

master="vip.creatis.insa-lyon.fr" #Default Master
lfc="lfc-biomed.in2p3.fr" #Default LFC


echo "<?xml version='1.0'?>" > $output_xml
echo "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">" >> $output_xml
echo "<platform version=\"3\">" >> $output_xml
echo '<AS id="AS_EGI" routing="full">' >> $output_xml


# tail -n +2 $machine_info => remove the header line
# sort -t\, -k1rn => sort entries in files by descending (-r) timestamps 
#                    first (-k1) numeric (-n) field when separated by ',' (-t\,)
# awk -F, '!a[$2]++'` => separate fields by , (-F,) 
#                     => keep only the first occurrence of second field (machine name)
# explanation in ex9 on http://www.theunixschool.com/2012/06/awk-10-examples-to-group-data-in-csv-or.html

# get the list of counteries available in logs from transfer_info.CSV Example:fr, it, nl.
array_countries=("$(awk -F',' '{if(NR!=1){count=split($1,a,".");print a[count]}}' $transfer_info | sort | uniq)" "$(awk -F',' '{if(NR!=1){count=split($2,a,".");print a[count]}}' $transfer_info | sort | uniq)")
# Remove duplicate records from array.
array_countries=($(printf '%s\n' "${array_countries[@]}" | sort -u))


# Loop on array_countries and create AS for each country.
for country in ${array_countries[@]}
do
  echo -e "\t<AS id=\"AS_"$country"\" routing=\"Cluster\">" >> $output_xml
  echo -e "\t\t<router id=\"gw_AS_"$country"\"/>" >> $output_xml
  
  # if country is France, add VIP server, LFC and Default SE
  if [[ "$country" == "fr" ]]; then
    echo -e '\t\t<host id="vip.creatis.insa-lyon.fr" power="10Gf" core="4"/>' >> $output_xml #Default Master
    echo -e '\t\t<host id="lfc-biomed.in2p3.fr" power="10Gf" core="4"/>' >> $output_xml #Default LFC

    #Check if default SE is in file_transfer.csv file, if yes:don't add defSE to platform.xml otherwise add it to platform.xml
    if grep -q $defSE $transfer_info
    then
      : #do nothing
    else
      echo -e '\t\t<host id="ccsrm02.in2p3.fr" power="10Gf" core="4"/>' >> $output_xml #Default SE 
    fi

  fi

  # Parse machine_info.csv to add realted hosts to the created AS.
  for line in `tail -n +2 $machine_info | sort -t\, -k1rn | awk -F, '!a[$2]++'`
  do   
    echo -n $line | awk -F',' -v cn="$country" '{if($6 == cn){print \
      "\t\t<host id=\""$2"\" power=\""$4"\" core=\""$3"\">\n" \
      "\t\t\t<prop id=\"closeSE\" value=\"" $7 "\"/>\n" \
      "\t\t</host>\n"\
      "\t\t<link id=\""$2"_link\" bandwidth=\""$5"\" latency=\"1ns\"/>\n" \
      "\t\t<host_link id=\""$2"\" up=\""$2"_link\" down=\""$2"_link\"/>\n"}}' >>$output_xml 
  done

  # Parse transfer_info.csv to add realted SE to the created AS.
  tail -n +2 $transfer_info | \
    awk -F',' -v cn="$country" '{gsub("ms","",$4); \
    if ($NF == "0"){count=split($2,a,".");if(a[count] == cn){bw=$3/$4;if (bw > arr_up[$2]){arr_up[$2]=bw};if ($2 in se){}{se[$2]};};} \
    else {count=split($1,a,".");if(a[count] == cn){bw=$3/$4;if (bw > arr_down[$1]){arr_down[$1]=bw};if ($1 in se){}{se[$1]};};}}
    END{\
      for (id in se){\
        if (id in arr_down && id in arr_up){ \
          print id" "arr_up[id]" "arr_down[id]} \
        else if (id in arr_up) {\
          print id" "arr_up[id]" "arr_up[id]} \
        else {print id" "arr_down[id]" "arr_down[id]}} \
    }' | awk '{ \
      print "\t\t<host id=\"" $1 "\" power=\"100Gf\"/>\n" \
      "\t\t<link id=\"" $1 "_UP\" bandwidth=\"" $2 "kBps\" latency=\"1ns\"/>\n" \
      "\t\t<link id=\"" $1 "_DOWN\" bandwidth=\"" $3 "kBps\" latency=\"1ns\"/>\n" \
      "\t\t<host_link id=\"" $1 "\" up=\"" $1 "_UP\" down=\"" $1 "_DOWN\"/>\n" 
  }' >>$output_xml

  # add a backbone for each AS.
  echo -e "\t\t<backbone id=\"backbone_"$country"\" bandwidth=\"100GBps\" latency=\"1ns\" sharing_policy=\"FATPIPE\"/>"  >>$output_xml  
  echo -e "\t</AS>\n" >> $output_xml

done





# Connecting ASs together 

nb_country=${#array_countries[@]}

# 1- create a link for each AS.
for ((i=0; i<$nb_country; i++)); do
  echo "<link id=\"link_"${array_countries[$i]}"\" bandwidth=\"10GBps\" latency=\"1.0E-4\"/>" >> $output_xml
done


# 2- Create routes between ASs using <ASroute>
for ((i=0; i<$nb_country; i++)); do
  for ((j=0; j<$nb_country; j++)); do
    if [[ "$i" == "$j" ]]; then
      :
    else
      echo -e "\t<ASroute src=\"AS_"${array_countries[$i]}"\" dst=\"AS_"${array_countries[$j]}"\" gw_src=\"gw_AS_"${array_countries[$i]}"\" \
      gw_dst=\"gw_AS_"${array_countries[$j]}"\" symmetrical=\"YES\">\n" \
      "\t\t<link_ctn id=\"link_"${array_countries[$i]}"\"/>\n" \
      "\t</ASroute>\n">> $output_xml
    fi
  done
done


echo "</AS>" >> $output_xml # End of EGI AS
echo "</platform>" >> $output_xml
    