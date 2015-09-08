#! /bin/bash -u

##############################################################################
# This script post-processes the information on file transfer to produce a   #
# CSV file with the average and maximum upload/download/overall bandwidth    #
# for each SE.                                                               #
# @Author: Frédéric SUTER                                                    #
# Company: CC-IN2P3                                                          #
##############################################################################

# local function to log the steps of execution of the script
function info {
  echo -e [`date +"%D %T"`] $*
}

initial=${1:-"standalone"}

if [ $initial == "initial" ]
then
    transfers="file_transfer.csv"
    se_bandwidth="se_bandwidth.csv"
    if [ ! -f "$se_bandwidth" ]
    then
	info "\tFile $se_bandwidth does not exist. Create it."
	echo "SE,AVG_ALL,MAX_ALL,AVG_DOWN,MAX_DOWN,AVG_UP,MAX_UP" > $se_bandwidth
    fi
else
    transfers="csv_files/file_transfer.csv"
    se_bandwidth="csv_files/se_bandwidth.csv"
    echo "SE,AVG_ALL,MAX_ALL,AVG_DOWN,MAX_DOWN,AVG_UP,MAX_UP" > $se_bandwidth
    info "Bandwidth recomputing" >> README
fi


tail -n +2 $transfers | \
  awk -F',' '{if ($5 > "0") { \
       bw=$5/$6; \
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
    }' >> $se_bandwidth
