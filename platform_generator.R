#!/usr/bin/Rscript
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER (2015-2016), Anchen CHAI (2016)            #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

#### Required R packages
library(XML)
library(plyr)
library(stats)
#### Parsing command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) < 1) {
  stop("Usage: platform_generator.R <workflow_name> [initial | standalone]", call.=FALSE)
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
transfers$Bandwidth <- transfers$FileSize/(pmax(0.1,(transfers$Time-1000)))

#Convert the real execution times from milliseconds to seconds
transfers$Time <- transfers$Time/1000

transfers$SE_SITE <- paste(transfers$Source,'_',transfers$SiteName,sep='')

# Store the list of identified grid sites and SEs
sites <- unique(workers$SiteName)
storage_elements <- unique(c(transfers[transfers$UpDown == 1,]$Destination, transfers[transfers$UpDown == 2,]$Source))


# Also Identify if there exists some SE used only for upload-tests. If there is, they have to be described though in 
# the platform file (with a minimum default bandwidth of 100kBps). 
upload_test_se <- unique(raw_transfers[raw_transfers$UpDown == 0,]$Destination)
upload_test_se <- upload_test_se[!upload_test_se %in% storage_elements]

workflow_name

if (length(upload_test_se) > 0) {
  upload_test_only = raw_transfers[(raw_transfers$UpDown == 0 & raw_transfers$Destination %in% upload_test_se),]
  upload_test_only$Bandwidth <- 100
  upload_test_only$SE_SITE <- paste(upload_test_only$Source,'_',upload_test_only$SiteName,sep='')
  # Include back the upload-test only SE(s) and their modified transfers into 
  # the data frames used for generation
  storage_elements <- c(storage_elements,upload_test_se)
  transfers = rbind(transfers,upload_test_only)
}

# Finally identify those that are declared as closeSE and check for inconsistencies in the transfers data frame:
#   * some SEs are used for upload without being declared as local
# If true, this indicates a problem in the logs, hence the generation has to be Bandwidth
local_ses <- unique(workers$CloseSE)
if (length(unique(transfers[!transfers$Destination %in% local_ses & transfers$UpDown==1,]$Destination))>0){
  warning(paste("WARNING: Some SEs are used for upload without being declared as local.", workflow_name))
}



#### Computing concurrency and apply corrective factor####

db <- read.csv(paste(wd,'db_dump.csv', sep="/"), header = TRUE, 
                      sep=' ',as.is=TRUE)
gate_db <- subset(db, Command=="gate.sh")
gate_downloads <- subset(transfers, UpDown == 2& JobType == "gate")
merge_downloads <- subset(transfers, UpDown == 2& JobType == "merge")


n_file <- 3
gate_downloads$Download_Start <- 0
gate_downloads$Download_End <- 0
merge_downloads$Download_Start <- 0
merge_downloads$Download_End <- 0

gate_downloads <- gate_downloads[order(gate_downloads$JobId),]  
gate_db <- gate_db[order(gate_db$JobId),]

for(j in 1:nrow(gate_db)){
  for(k in 1:n_file){
    if(k==1){
      gate_downloads[(j-1)*3+k,]$Download_Start = round(gate_db[j,]$DownloadStartTime)
    }
    else{
      gate_downloads[(j-1)*3+k,]$Download_Start = 
        round(gate_downloads[(j-1)*3+k-1,]$Download_End)
    } 
    gate_downloads[(j-1)*3+k,]$Download_End = 
      gate_downloads[(j-1)*3+k,]$Download_Start + round(gate_downloads[(j-1)*3+k,]$Time)
  }
}

gate_downloads$concurrency <- 1

#remove 1s as latency
gate_downloads$Download_Start <- gate_downloads$Download_Start + 1

# divide each transfer into 50 intervals to estimate the nominal bandwidth (only for Gate downloads)
n_interval <- 50
for(i in 1:nrow(gate_downloads)){
  conc <- vector(mode="numeric", length=n_interval)
  step <- (gate_downloads[i,]$Download_End - gate_downloads[i,]$Download_Start )/n_interval
  for(s in 1: n_interval){
    conc[s] <- nrow(subset(gate_downloads, SE_SITE == gate_downloads[i,]$SE_SITE
                                         &FileSize == gate_downloads[i,]$FileSize
                                         &Download_Start <= (step*(s-1)+gate_downloads[i,]$Download_Start)
                                         &Download_End >= (step*(s-1)+gate_downloads[i,]$Download_Start)))
  }

  gate_downloads[i,]$concurrency <- n_interval/sum(1/conc)
}

# for merge download transfers, the concurrency is considered as 1
merge_downloads$concurrency <- 1

df_download <- rbind(gate_downloads, merge_downloads)
df_download$Bandwidth_improved <- df_download$Bandwidth * df_download$concurrency


##############################

# Compute the respective average bandwidth from (then to) these SE to (then from)each grid site. Let ddply produce NaN
# entries. The rationale is that during the simulation the LFC can pick a SE for download input that was not selected 
# during the real execution. To circumvent this, we add a default bandwidth for the missing connections. This value 
# is set to the maximum observed bandwidth.

SE_to_site_bandwidth = ddply(df_download, c("Source","SiteName"), summarize, 
                             AvgBandwidth=round(mean(Bandwidth),2), MaxBandwidth=max(Bandwidth_improved), .drop=FALSE)


if (TRUE %in% is.nan(SE_to_site_bandwidth$AvgBandwidth)) {
  SE_to_site_bandwidth[is.nan(SE_to_site_bandwidth$AvgBandwidth),]$AvgBandwidth <- 
    max(transfers[transfers$UpDown==2,]$Bandwidth)
}

if (TRUE %in% is.infinite(SE_to_site_bandwidth$MaxBandwidth)) {
  SE_to_site_bandwidth[is.infinite(SE_to_site_bandwidth$MaxBandwidth),]$MaxBandwidth <- 
    max(transfers[transfers$UpDown==2,]$Bandwidth)
}


site_to_SE_bandwidth = ddply(transfers[transfers$UpDown != 2,], c("Destination","SiteName"),summarize, 
                             AvgBandwidth=round(mean(Bandwidth),2),MaxBandwidth=max(Bandwidth),.drop=FALSE)

if (TRUE %in% is.nan(site_to_SE_bandwidth$AvgBandwidth)) {
  site_to_SE_bandwidth[is.nan(site_to_SE_bandwidth$AvgBandwidth),]$AvgBandwidth <- 
    max(transfers[transfers$UpDown != 2,]$Bandwidth)
}

if (TRUE %in% is.infinite(site_to_SE_bandwidth$MaxBandwidth)) {
  site_to_SE_bandwidth[is.infinite(site_to_SE_bandwidth$MaxBandwidth),]$MaxBandwidth <- 
    max(transfers[transfers$UpDown != 2,]$Bandwidth)
}

#### Generation of the XML tree
# Creation and header
platform_out=c("Avg_Fatpipe_","Max_Shared_")
for(n in 1:length(platform_out)){
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
    t$addTag("backbone",attrs=c(id="Services_backbone", bandwidth="100Gbps", latency="750us"))

    t$addTag("host", attrs=c(id="vip.creatis.insa-lyon.fr", speed="5Gf",core="48"))
    t$addTag("link", attrs=c(id="vip.creatis.insa-lyon.fr_link", bandwidth="10Gbps", latency="500us",
                             sharing_policy="FULLDUPLEX"))
    t$addTag("host_link", attrs=c(id="vip.creatis.insa-lyon.fr", up="vip.creatis.insa-lyon.fr_link_UP",
                                  down="vip.creatis.insa-lyon.fr_link_DOWN"))
      
    t$addTag("host", attrs=c(id="lfc-biomed.in2p3.fr", speed="5Gf", core="48"))
    t$addTag("link", attrs=c(id="lfc-biomed.in2p3.fr_link", bandwidth="10Gbps", latency="500us", 
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
      t$addTag("backbone",attrs=c(id=paste(i,"backbone", sep="_"), bandwidth="100Gbps", latency="750us"))
      
      w = workers[workers$SiteName == i,]
      for (j in 1:nrow(w)){
        # Declaration of the host and its close SE
        t$addTag("host", attrs=c(id=w[j,2], speed=w[j,4], core=w[j,3]), close=FALSE)
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

    #### Declare links between ASes with policy Avg_Fatpipe
    if(n == 1){
      t$addTag("link", attrs=c(id="service_link", bandwidth="10Gbps", latency="500us"))

      for (i in 1:nrow(SE_to_site_bandwidth)){
        t$addTag("link", attrs= c(id=paste(SE_to_site_bandwidth[i,1], SE_to_site_bandwidth[i,2], sep="-"),
                                  bandwidth=paste(SE_to_site_bandwidth[i,3],"kBps", sep=""),
                                  latency="500us",sharing_policy="FATPIPE"))
      } 

      for (i in 1:nrow(site_to_SE_bandwidth)){
        t$addTag("link", attrs= c(id=paste(site_to_SE_bandwidth[i,2], site_to_SE_bandwidth[i,1], sep="-"),
                                  bandwidth=paste(site_to_SE_bandwidth[i,3],"kBps", sep=""),
                                  latency="500us",sharing_policy="FATPIPE"))
      } 
    }

    #### Declare links between ASes with policy Max_Shared
    if(n == 2){
      t$addTag("link", attrs=c(id="service_link", bandwidth="10Gbps", latency="500us"))

      for (i in 1:nrow(SE_to_site_bandwidth)){
        t$addTag("link", attrs= c(id=paste(SE_to_site_bandwidth[i,1], SE_to_site_bandwidth[i,2], sep="-"),
                                  bandwidth=paste(SE_to_site_bandwidth[i,4],"kBps", sep=""),
                                  latency="500us",sharing_policy="SHARED"))
      } 

      for (i in 1:nrow(site_to_SE_bandwidth)){
        t$addTag("link", attrs= c(id=paste(site_to_SE_bandwidth[i,2], site_to_SE_bandwidth[i,1], sep="-"),
                                  bandwidth=paste(site_to_SE_bandwidth[i,4],"kBps", sep=""),
                                  latency="500us",sharing_policy="SHARED"))
      } 
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

    for (i in 1:nrow(SE_to_site_bandwidth)){
      if (SE_to_site_bandwidth[i,2] %in% 
            site_to_SE_bandwidth[site_to_SE_bandwidth$Destination == SE_to_site_bandwidth[i,1],2]){
        sym = "NO"
      } else {
        sym="YES"
      }
      t$addTag("ASroute", attrs=c(src=paste("AS",SE_to_site_bandwidth[i,1], sep="_"), 
                                  dst=paste("AS", SE_to_site_bandwidth[i,2], sep="_"),
                                  gw_src = SE_to_site_bandwidth[i,1], 
                                  gw_dst=paste("AS",SE_to_site_bandwidth[i,2], "router", sep="_"),
                                  symmetrical=sym), close=FALSE)
      t$addTag("link_ctn", attrs=c(id=paste(SE_to_site_bandwidth[i,1], SE_to_site_bandwidth[i,2], sep="-")))
      t$closeTag()    
    }

    for (i in 1:nrow(site_to_SE_bandwidth)){
      if (site_to_SE_bandwidth[i,2] %in% 
            SE_to_site_bandwidth[SE_to_site_bandwidth$Source == site_to_SE_bandwidth[i,1],2]){
        sym = "NO"
      } else {
        sym="YES"
      }
      t$addTag("ASroute", attrs=c(src=paste("AS", site_to_SE_bandwidth[i,2], sep="_"),
                                  dst=paste("AS",site_to_SE_bandwidth[i,1], sep="_"),
                                  gw_src=paste("AS",site_to_SE_bandwidth[i,2], "router", sep="_"),
                                  gw_dst=site_to_SE_bandwidth[i,1], symmetrical=sym), close=FALSE)
      t$addTag("link_ctn", attrs=c(id=paste(site_to_SE_bandwidth[i,2], site_to_SE_bandwidth[i,1], sep="-")))
      t$closeTag()    
    }

    # Close the initial <AS> tag
    t$closeTag()

    # Save the XML tree to disk
    cat(saveXML(t), file=paste(output_dir,"/AS_",platform_out[n],"platform_",workflow_name,".xml", sep=""))
}
