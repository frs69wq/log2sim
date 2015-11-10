#! /bin/bash -u
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER, Mohammad Mahdi BAZM (2015)                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

workflow_dir=${1:? Workflow directory name is mandatory!}

worker_nodes=worker_nodes.csv
file_transfer=file_transfer.csv
deployment_file=deployment_${workflow_dir}.xml
#application_file=${4:? application_file.txt is needed.}
platform_file=platform_${workflow_dir}_max_asymmetric.xml
output=${workflow_dir}_summary.html #output file in format HTML.

cat <<EOT > $output
<!DOCTYPE html>
<html>
<head>
    <title>Summary of $workflow_dir</title>
    <script src="../../utilstabcontent.js" type="text/javascript"></script>
    <link href="../../utils/tabcontent.css" rel="stylesheet" type="text/css" />
    <style>
        table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
        }
        th, td {
        padding: 5px;
        text-align: center;
    }
    </style>
</head>
<body>
        <div style="width:100%; margin: 0 auto; padding: 0px 0 40px;"> 
        <ul class="tabs" data-persist="true">
            <li><a href="#view1">Worker Nodes</a></li>
            <li><a href="#view2">File Transfers</a></li>
            <li><a href="#view3">Deployment File</a></li>
            <!--li><a href="#view4">Application File</a></li-->
            <li><a href="#view5">Platform File</a></li>
        </ul>
        <div class="tabcontents">            
EOT
################################# Write to Worker nodes TAB ##############################
echo -e '<div id="view1">\n'\
        '<b>Worker Nodes</b>\n'\
        '<table  style="width:100%;background-color:#F7F8E0;border-radius:10px;-moz-border-radius:10px;-webkit-border-radius:10px;">\n' >> $output
print_header=false
while read INPUT ; do
  if $print_header;then
    echo "<tr><th>$INPUT" | sed -e 's/:[^,]*\(,\|$\)/<\/th><th>/g' >> $output
    #print_header=false
  fi
  echo "<tr><td>${INPUT//,/</td><td>}</td></tr>" >> $output
done < $worker_nodes ;
echo -e "\t\t\t</table>\n" \
        "\t\t</div>" >> $output

################################### Write to File transfer TAB ##############################
#add view2 tab to the html file.
echo -e '\t\t<div id="view2">\n' \
        '\t\t\t<b>File Transfers</b>\n' \
        '\t\t\t<table  style="width:100%;background-color:#F7F8E0;border-radius:10px;-moz-border-radius:10px;-webkit-border-radius:10px;">\n' >> $output
while read INPUT ; do
  if $print_header;then
    echo "<tr><th>$INPUT" | sed -e 's/:[^,]*\(,\|$\)/<\/th><th>/g' >> $output #write head of table
    #print_header=false
  fi
  echo "<tr><td>${INPUT//,/</td><td>}</td></tr>" >> $output #write rows of table in the output file
done < $file_transfer ;
echo -e "\t\t\t</table>\n" \
        "\t\t</div>" >> $output

################################ write to Deployment file TAB ################################
echo -e '<div id="view3">\n'\
     '<xmp>\n' >> $output
while read line ; do
    echo $line >> $output
done < $deployment_file;
echo "</xmp>" >> $output

echo -e "\t\t</div>" >> $output

################################ write to Platform file TAB ################################
echo -e '<div id="view5">\n'\
     '<xmp>\n' >> $output
while read line ; do
    echo $line >> $output
done < $platform_file;
echo "</xmp>" >> $output

echo -e "\t\t</div>" >> $output          

##############################################################################################

#End of HTML document
echo -e "\t\t</div>\n"\
        "\t</div>\n" \
        "</body>\n"\
        "</html>\n" >> $output
#open $output
