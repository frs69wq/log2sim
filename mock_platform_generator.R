#!/usr/bin/Rscript
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER (2016)                                     #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

#### Required R packages
library(XML)
library(plyr)
#### Parsing command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) < 1) {
  stop("Usage: mock_generator.R <workflow_name> [initial | standalone]", call.=FALSE)
}

workflow_name=args[1]

if (length(args) >= 2) {
  initial = args[2]
} else {
  initial = "standalone"
}

if (initial == "initial"){
  wd="."
  output_dir="."
} else {
  wd="csv_files"
  output_dir="simgrid_files"
}

#### Data preparation
# Get information about worker nodes
workers <- read.csv(paste(wd,'worker_nodes.csv', sep="/"), header=TRUE, sep=',', as.is=TRUE)

# Get information about file transfers 
raw_transfers <- read.csv(paste(wd,'file_transfer.csv', sep="/"), header = TRUE, sep=',',as.is=TRUE)

# Remove the upload-tests and the small downloads of 11 bytes made by merge
transfers=raw_transfers[raw_transfers$FileSize >12,]

# Compute the observed bandwidth for each individual file transfer
# remove 1 sec from the transfer time (dispatched as network/control latency)
transfers$Bandwidth <- transfers$FileSize/(pmax(1,(transfers$Time-1000)))

# Store the list of identified grid sites and SEs
sites <- unique(workers$SiteName)
storage_elements <- unique(c(transfers[transfers$UpDown == 1,]$Destination, transfers[transfers$UpDown == 2,]$Source))

SE_to_site = ddply(transfers[transfers$UpDown==2,], c("Source","SiteName"), summarize, Bandwidth="10Gbps",.drop=FALSE)
site_to_SE = ddply(transfers[transfers$UpDown!=2,], c("Destination","SiteName"),summarize, Bandwidth="10Gbps",
                   .drop=FALSE)

#### Generation of the XML tree
# Creation and header
t = xmlTree("platform", attrs=c(version="4"), 
            dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd"')
t$addTag("AS", attrs=c(id=workflow_name, routing="Full"), close=FALSE)

# Definition of a first AS called 'Services' that comprises
#     * a router
#     * a backbone
#     * the master server 'vip.creatis.insa-lyon.fr'
#     * the logical file catalog 'lfc-biomed.in2p3.fr'

t$addTag("AS", attrs=c(id="Services", routing="Cluster"), close=FALSE)
t$addTag("router", attrs=c(id="Services_router"))
t$addTag("backbone",attrs=c(id="Services_backbone", bandwidth="10Gbps", latency="750us"))

t$addTag("host", attrs=c(id="vip.creatis.insa-lyon.fr", speed="5Gf",core="48"))
t$addTag("link", attrs=c(id="vip.creatis.insa-lyon.fr_link", bandwidth="1Gbps", latency="500us", 
                         sharing_policy="FULLDUPLEX"))
t$addTag("host_link", attrs=c(id="vip.creatis.insa-lyon.fr", up="vip.creatis.insa-lyon.fr_link_UP",
                              down="vip.creatis.insa-lyon.fr_link_DOWN"))

t$addTag("host", attrs=c(id="lfc-biomed.in2p3.fr", speed="5Gf", core="48"))
t$addTag("link", attrs=c(id="lfc-biomed.in2p3.fr_link", bandwidth="1Gbps", latency="500us", 
                         sharing_policy="FULLDUPLEX"))
t$addTag("host_link", attrs=c(id="lfc-biomed.in2p3.fr", up="lfc-biomed.in2p3.fr_link_UP",
                              down="lfc-biomed.in2p3.fr_link_DOWN"))
t$closeTag()

for (i in sites){
  # Definition of an AS for each identified grid site that comprises
  #     * a router
  #     * a backbone
  #     * all the used worker nodes that belong to this site
  
  t$addTag("AS", attrs=c(id=paste("AS",i, sep="_"), routing="Cluster"), close=FALSE)
  t$addTag("router", attrs=c(id=paste("AS",i,"router", sep="_")))
  t$addTag("backbone",attrs=c(id=paste(i,"backbone", sep="_"), bandwidth="10Gbps", latency="750us"))
  
  w = workers[workers$SiteName == i,]
  for (j in 1:nrow(w)){
    # Declaration of the host and its close SE
    t$addTag("host", attrs=c(id=w[j,2], power=w[j,4], core=w[j,3]), close=FALSE)
    t$addTag("prop", attrs=c(id="closeSE", value=w[j,8]))
    t$closeTag()
    
    # Declaration of the full-duplex link that connects the host to the AS's backbone
    t$addTag("link", attrs=c(id=paste(w[j,2],"link",sep="_"), bandwidth=w[j,5], latency="500us", 
                             sharing_policy="FULLDUPLEX"))
    t$addTag("host_link", attrs=c(id=w[j,2], up=paste(w[j,2],"link_UP",sep="_"), 
                                  down=paste(w[j,2], "link_DOWN", sep="_")))  
  }  
  t$closeTag()
}

for (i in storage_elements){
  # Definition of an AS for each SE. Such AS only comprises the node hosting the SE service. The routing
  # method for this AS is 'None'
  t$addTag("AS", attrs=c(id=paste("AS",i,sep="_"), routing="None"), close=FALSE)
  t$addTag("host", attrs=c(id=i, speed="5Gf", core="48"))
  t$closeTag()
}

#### Declare links between ASes
t$addTag("link", attrs=c(id="service_link", bandwidth="10Gbps", latency="500us"))
for (i in 1:nrow(SE_to_site)){
  t$addTag("link", attrs= c(id=paste(SE_to_site[i,1], SE_to_site[i,2], sep="-"),
                            bandwidth=SE_to_site[i,3], latency="500us"))
} 

for (i in 1:nrow(site_to_SE)){
  t$addTag("link", attrs= c(id=paste(site_to_SE[i,2], site_to_SE[i,1], sep="-"),
                            bandwidth=site_to_SE[i,3], latency="500us"))
} 

#### Declare the routing between ASes
# from the 'Services' AS to all the other ASes (grid sites and storage elements)
for (i in sites){
  t$addTag("ASroute", attrs=c(src="Services", dst=paste("AS", i, sep="_"), gw_src="Services_router", 
                              gw_dst=paste("AS",i,"router", sep="_")), close=FALSE)
  t$addTag("link_ctn", attrs=c(id="service_link"))
  t$closeTag()  
}

for (i in storage_elements){
  t$addTag("ASroute", attrs=c(src="Services", dst=paste("AS", i, sep="_"), gw_src="Services_router", gw_dst=i), 
           close=FALSE)
  t$addTag("link_ctn", attrs=c(id="service_link"))
  t$closeTag()  
}

for (i in 1:nrow(SE_to_site)){
  if (SE_to_site[i,2] %in% site_to_SE[site_to_SE$Destination == SE_to_site[i,1],2]){
    sym = "NO"
  } else {
    sym="YES"
  }
  t$addTag("ASroute", attrs=c(src=paste("AS",SE_to_site[i,1], sep="_"), dst=paste("AS", SE_to_site[i,2], sep="_"),
                              gw_src = SE_to_site[i,1], gw_dst=paste("AS",SE_to_site[i,2], "router", sep="_"),
                              symmetrical=sym), close=FALSE)
  t$addTag("link_ctn", attrs=c(id=paste(SE_to_site[i,1], SE_to_site[i,2], sep="-")))
  t$closeTag()    
}

for (i in 1:nrow(site_to_SE)){
  if (site_to_SE[i,2] %in% SE_to_site[SE_to_site$Source == site_to_SE[i,1],2]){
    sym = "NO"
  } else {
    sym="YES"
  }
  t$addTag("ASroute", attrs=c(src=paste("AS", site_to_SE[i,2], sep="_"), dst=paste("AS",site_to_SE[i,1], sep="_"),
                              gw_src=paste("AS",site_to_SE[i,2], "router", sep="_"), gw_dst=site_to_SE[i,1], 
                              symmetrical=sym), close=FALSE)
  t$addTag("link_ctn", attrs=c(id=paste(site_to_SE[i,2], site_to_SE[i,1], sep="-")))
  t$closeTag()    
}

# Close the initial <AS> tag
t$closeTag()

# Save the XML tree to disk
cat(saveXML(t), file=paste(output_dir,"/mock_platform_",workflow_name,".xml", sep=""))