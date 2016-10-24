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
############################### Parsing command line arguments #########################################
args = commandArgs(trailingOnly=TRUE)
if (length(args) < 1) {
  stop("Usage: platform_generator.R <workflow_name> [initial | standalone]", call.=FALSE)
} else {
  workflow_name=args[1]

  if (length(args) >= 2) {
    initial = args[2]
  } else {
    initial = "standalone"
  }

  if (initial == "initial"){
    wd="./"
    output_dir="./"
  } else {
    wd="csv_files/"
    output_dir="simgrid_files/"
  }
}
############################### Operations on worker_nodes.csv #########################################
split_and_rewrite_hostname <-function(df){
  df$hostname <- sapply(df$Name, function (x) strsplit(x,"[.]")[[1]][1])
  df$prefix <- gsub('[[:digit:]]+', '', df$hostname)
  df$radical <- as.numeric(gsub('[[:alpha:]-]+', '', df$hostname))
  df$suffix <- sapply(df$Name, function (x) gsub(strsplit(x,"[.]")[[1]][1],'',x))
  df$Name <- paste0(df$prefix,df$radical,df$suffix)
  df$hotname<-NULL
  df
}

get_worker_nodes <- function(file_name){
  # read the CSV file
  df <- subset(read.csv(file_name, header=TRUE, sep=',', as.is=TRUE), select=-c(Timestamp, Country))
  df <- split_and_rewrite_hostname(df)
  df$MIPS = round(as.numeric(gsub("Mf","", df$MIPS)),-1)
  df$ClusterName = paste0(df$prefix,"-", df$Core,"cores-at-",df$MIPS,"MIPS",df$suffix)

  df$NetSpeed = sapply(df$NetSpeed, function (x) 
    if (grepl("Gbps", x)) as.numeric(gsub("Gbps","000", x))
    else as.numeric(gsub("Mbps","", x)))
  df
}

build_clusters <- function(workers){
  df <- ddply(workers, .(prefix, suffix, Core, MIPS, NetSpeed, SiteName, CloseSE), summarize, 
                      radical=list(sort(radical)))
  df$radical= sapply(df$radical, function(x) paste(as.list(x), collapse=","))
  df$name = paste0(df$prefix,"-", df$Core,"cores-at-",df$MIPS,"MIPS",df$suffix)
  df
}
############################### Operations on db_dump.csv ##############################################
get_job_start_times <- function (file_name){
  df <- subset(read.csv(file_name, header = TRUE, sep=' ',as.is=TRUE), 
               select = c(JobId, DownloadStartTime,UploadStartTime))
  df$End <-0
  rename(df,c("DownloadStartTime" = "Start", "UploadStartTime" = "Start_Upload"))
}
############################### Operations on file_transfer.csv ########################################
update_start_end_times <-function(df){
  gate_count <- (nrow(df)-5)/6 
  for(j in 1:(nrow(df)-gate_count-5)){
    # Each Gate job corresponds to 5 transfers in the data frame:
    #   - upload test
    #   - upload result
    #   - download of 3 inputs
    if (j %%5 == 2) # Upload of partial result, use StartUpload as start time
      df[j,]$Start = df[j,]$Start_Upload
    if (j %% 5 == 3) # Skip the interleaved upload to get the end time of upload_test as start time of the first download
      df[j,]$Start = df[j-2,]$End
    if (!((j %% 5) %in% c(1,2,3))) # use end time of previous transfers for the other ones
      df[j,]$Start = df[j-1,]$End
    df[j,]$End = df[j,]$Start + df[j,]$Time    
  }
  for (i in 1:(5+gate_count)) {
    j<-5*gate_count+i
    # The merge job corresponds to 5 + gate_count  transfers in the data frame:
    #   - upload test
    #   - 2 uploads (partial result and one of 11 bytes)
    #   - download of 2 inputs
    #   - one download per GATE job
    if (i == 2) # Upload of partial result, use StartUpload as start time
      df[j,]$Start = df[j,]$Start_Upload
    if (i == 4) # Skip the uploads
      df[j,]$Start = df[j-3,]$End
    if (!(i %in% c(1,2,4))) # use end time of previous transfers for the other ones
      df[j,]$Start = df[j-1,]$End
    
    df[j,]$End = df[j,]$Start + df[j,]$Time    
  }
  subset(df, select=-c(Start_Upload)) # This field is useless now, get rid of it
}

set_file_types <- function(df){
  gate_count <- (nrow(df)-5)/6
  df$File_Type <- "Partial download"
  for(j in 1:(nrow(df)-gate_count-5)){
    df[j,]$File_Type <-switch (j%%5, "Upload Test", "Partial Upload", "Wrapper", "Release")
    if (j%%5==0)
      df[j,]$File_Type <- "Input"
    }
  for (i in 1:5) {
     j<-5*gate_count+i
     df[j,]$File_Type <-switch (i, "Upload Test", "Partial Upload", "Small Upload", "Release", "Input")
   }
  df
}

get_transfers <- function(file_name){
  # read the CSV file
  df <- subset(read.csv(file_name, header=TRUE, sep=',', as.is=TRUE), select=-c(Timestamp))
  #Convert the execution times from milliseconds to seconds
  df$Time <- round(df$Time/1000,2)
  # Compute the observed bandwidth for each individual file transfer
  # remove 1 sec from the transfer time (dispatched as network/control latency)
  df$Bandwidth_in_bps  <- (8*df$FileSize)/pmax(0.001,(df$Time-1))
  
  # Construct the link name:
  # if UpDown == 2 (Download), its Source_SiteName
  # Otherwise (Upload-test or Upload), it's SiteName_Destination
  df$Link <- apply(df[,c(2:3,7:8)], 1, 
                   function(x) if (x[4]==2) paste0(x[1],'-',x[3])
                               else         paste0(x[3],'-',x[2]))

  # Construct the cluster name
  df$Cluster <- apply(df[,c(2:3,8)],1, function(x)
    if (x[3]==2)  workers[workers$hostname == strsplit(x[2],"[.]")[[1]][1],]$ClusterName
    else          workers[workers$hostname == strsplit(x[1],"[.]")[[1]][1],]$ClusterName)

  # Construct the internal link name:
  # if UpDown == 2 (Download), its Source_SiteName_Cluster
  # Otherwise (Upload-test or Upload), it's Cluster_SiteName_Destination
  df$ClusterLink <- apply(df[,c(2:3,7:8,11)], 1,
                   function(x) if (x[4]==2) paste0(x[3],'-',x[1],'-',x[5])
                               else         paste0(x[5],'-',x[3],'-',x[2]))
  
  df <- merge(df, get_job_start_times(paste0(wd,'db_dump.csv')), by=c("JobId"))
  df <- update_start_end_times(df)
  df <- set_file_types(df)

  # SANITY CHECK: Are some SEs used for upload without being declared as local?
  # If true, this indicates a problem in the logs. Raise a warning.
  if (length(unique(df[!df$Destination %in% local_ses & df$UpDown==1,]$Destination))>0){
    warning(paste("WARNING: Some SEs are used for upload without being declared as local.", workflow_name))
  }
  df
}

correct_bandwidth <-function(df){
  df$concurrency_on_link <- 1
  df$concurrency_on_cluster_link <- 1
  for(i in 1:nrow(df)){
    # Get the begin and end time of this transfer. 
    # Add 1 second (latency) to the start time
    begin <- df[i,]$Start + 1
    end <- df[i,]$End
    
    # Determine the number of intervals in which split the transfer time
    # NEW: adapt the number to the duration. At least 1 interval and at most 50
    # Transfers shorter than 5 seconds are split in 10 times their duration 
    # Example: a transfer of 2 seconds will be split in 20 intervals
    n_interval <- max(1,min(50,ceiling((10*(end-begin)))))
    
    # Set the length of each interval
    step <- (end - begin) / n_interval
    
    # Get the set of concurrent transfers once for all. They have to begin 
    # before the end  AND end after the beginning of the current transfer
    others_on_link <- 
      subset(df, Link == df[i,]$Link & (Start + 1) <= end & End >= begin)
    others_on_cluster_link <- 
      subset(df, ClusterLink == df[i,]$ClusterLink & (Start + 1) <= end & End >= begin)
    if (n_interval == 1){
      # if there is only one interval to cover, we're done
      df[i,]$concurrency_on_link <- nrow(others_on_link)
      df[i,]$concurrency_on_cluster_link <- nrow(others_on_cluster_link)
    } else {
      cur_concurrency_on_link <- 0
      cur_concurrency_on_cluster_link <- 0
      if (nrow(others_on_link) > 1) {
        # This makes sense only if there is concurrency
        for(s in 1: n_interval){
          # set the end of the current interval
          end <- begin + step
          # accumulate the inverse of the number of concurrent transfers over
          # this interval
          cur_concurrency_on_link <- cur_concurrency_on_link +
            1/(nrow(subset(others_on_link, (Start +1) <= end & End >= begin)))
          cur_concurrency_on_cluster_link <- cur_concurrency_on_cluster_link +
            1/(nrow(subset(others_on_cluster_link, (Start +1) <= end & End >= begin)))
          # update begin for the next round
          begin <- end
        }
        # End of formula: N/sum_{i=1}^{N}(1/C_i)
        df[i,]$concurrency_on_link <- n_interval/cur_concurrency_on_link
        df[i,]$concurrency_on_cluster_link <- n_interval/cur_concurrency_on_cluster_link
      }
    }
  }
  df$Corr_Bandwidth_in_bps  <-  df$Bandwidth_in_bps* df$concurrency_on_link
  df$Corr_Bandwidth_by_cluster_in_bps  <-  df$Bandwidth_in_bps* df$concurrency_on_cluster_link
  df
}

get_bandwidths_by_SE <- function(Transfers){
  to_SE <- ddply(Transfers[Transfers$UpDown != 2,], c("Destination"), summarize,
              Avg=round(mean(Bandwidth_in_bps)), Max=round(max(Bandwidth_in_bps)))
  names(to_SE) = c("SE","Avg_to","Max_to")
  from_SE <- ddply(Transfers[Transfers$UpDown == 2,], c("Source"), summarize, 
                   Avg=round(mean(Bandwidth_in_bps)), Max=round(max(Bandwidth_in_bps)))
  names(from_SE) = c("SE","Avg_from","Max_from")
  df <- merge(to_SE, from_SE, all=TRUE)
  df$Avg <- apply(df[,c(2,4)], 1, function(s)
    if (is.na(s[1])) s[2] else if (is.na(s[2])) s[1] else (s[1]+s[2])/2)
  df$Max <- apply(df[,c(3,5)], 1, function(s)
    if (is.na(s[1])) s[2] else if (is.na(s[2])) s[1] else (max(s[1], s[2])))
  df$Mock_1G = 1e9
  df$Mock_10G =1e10
  df
}

get_bandwidths_by_link <- function(Transfers){
  df <- ddply(Transfers, c("Link","File_Type"), summarize, count=length(Link),
              Avg=round(mean(Bandwidth_in_bps)), Max=round(max(Bandwidth_in_bps)), 
              Corr_Avg=round(mean(Corr_Bandwidth_in_bps)),
              Corr_Max=round(max(Corr_Bandwidth_in_bps)), 
              Mock_1G=1e9, Mock_10G=1e10)
  release <- subset(df, File_Type == 'Release')
  others <- ddply(subset(df, !(Link %in% release$Link)), .(Link), function(x) x[which.max(x$Max),])
  rbind(release,others)
}

get_bandwidths_by_clusterlink <- function(Transfers){
  df <- ddply(Transfers, c("ClusterLink","File_Type"), summarize, count=length(ClusterLink),
              Avg=round(mean(Bandwidth_in_bps)), Max=round(max(Bandwidth_in_bps)), 
              Corr_Avg=round(mean(Corr_Bandwidth_by_cluster_in_bps)),
              Corr_Max=round(max(Corr_Bandwidth_by_cluster_in_bps)),
              Mock_1G=1e9, Mock_10G=1e10, Link=unique(Link))
  release <- subset(df, File_Type == 'Release')
  others <- ddply(subset(df, !(ClusterLink %in% release$ClusterLink)), .(ClusterLink),
                  function(x) x[which.max(x$Max),])
  rbind(release,others)
}

select_routes <- function(Sites, SEs){
  site_to_SE <- merge(Sites, SEs)
  names(site_to_SE) <- c("src", "dst")
  site_to_SE$Link <- paste(site_to_SE$src, site_to_SE$dst, sep='-')
  site_to_SE$ReverseLink <- paste(site_to_SE$dst, site_to_SE$src, sep='-')
  site_to_SE$src <- paste0("AS_", site_to_SE$src)
  site_to_SE$gw_src <- paste0(site_to_SE$src, "_router")
  site_to_SE$gw_dst <- site_to_SE$dst
  site_to_SE$dst <- paste0("AS_", site_to_SE$dst)
  site_to_SE <- subset (site_to_SE, Link %in% bandwidth_by_link$Link)

  SE_to_site <- merge(SEs, Sites)
  names(SE_to_site) <- c("src", "dst")
  SE_to_site$Link <- paste(SE_to_site$src, SE_to_site$dst, sep='-')
  SE_to_site$ReverseLink <- paste(SE_to_site$dst, SE_to_site$src, sep='-')
  SE_to_site <- subset (SE_to_site, Link %in% bandwidth_by_link$Link)
  SE_to_site$dst <- paste0("AS_", SE_to_site$dst)
  SE_to_site$gw_src <- SE_to_site$src
  SE_to_site$src <- paste0("AS_", SE_to_site$src)
  SE_to_site$gw_dst <- paste0(SE_to_site$dst, "_router")

  site_to_SE$symmetrical <- sapply(site_to_SE$ReverseLink, function(x) if (x %in% SE_to_site$Link) "NO" else "YES")
  SE_to_site$symmetrical <- sapply(SE_to_site$ReverseLink, function(x) if (x %in% site_to_SE$Link) "NO" else "YES")

  rbind(site_to_SE, SE_to_site)
}

# FIXME: To keep?
select_cluster_routes <- function(Clusters, SEs){
  site_name <- unique (Clusters$SiteName)
  cluster_names <- Clusters$name 

  SE_routers <- laply(SEs, function (x) paste0(site_name,"-", x))
  cluster_to_SE <- merge(cluster_names, SE_routers)
  names(cluster_to_SE) <- c("src", "dst")
  cluster_to_SE$Link <- paste(cluster_to_SE$src, cluster_to_SE$dst, sep='-')
  cluster_to_SE$ReverseLink <- paste(cluster_to_SE$dst, cluster_to_SE$src, sep='-')
  cluster_to_SE$src <- as.character(cluster_to_SE$src)
  cluster_to_SE$dst <- as.character(cluster_to_SE$dst)
  cluster_to_SE$gw_src <- paste0(cluster_to_SE$src, "_router")
  cluster_to_SE$gw_dst <- paste0(cluster_to_SE$dst, "_router")
  cluster_to_SE <- subset (cluster_to_SE, Link %in% bandwidth_by_clusterlink$ClusterLink)
  
  SE_to_cluster <- merge(SE_routers, cluster_names)
  names(SE_to_cluster) <- c("src", "dst")
  SE_to_cluster$Link <- paste(SE_to_cluster$src, SE_to_cluster$dst, sep='-')
  SE_to_cluster$ReverseLink <- paste(SE_to_cluster$dst, SE_to_cluster$src, sep='-')
  SE_to_cluster <- subset (SE_to_cluster, Link %in% bandwidth_by_clusterlink$ClusterLink)
  SE_to_cluster$src <- as.character(SE_to_cluster$src)
  SE_to_cluster$dst <- as.character(SE_to_cluster$dst)
  SE_to_cluster$gw_src <- paste0(SE_to_cluster$src, "_router")
  SE_to_cluster$gw_dst <- paste0(SE_to_cluster$dst, "_router")

  cluster_to_SE$symmetrical <- sapply(cluster_to_SE$ReverseLink, function(x) if (x %in% SE_to_cluster$Link) "NO" else "YES")
  SE_to_cluster$symmetrical <- sapply(SE_to_cluster$ReverseLink, function(x) if (x %in% cluster_to_SE$Link) "NO" else "YES")

  rbind(cluster_to_SE, SE_to_cluster)
}

############################### Generation of the different parts of an XML file #######################
Service_nodes_in_single_AS <- function(){
  vip      <- newXMLNode("host", attrs=c(id="vip.creatis.insa-lyon.fr", speed="5Gf",core="48"))
  vip_link <- newXMLNode("link", attrs=c(id="vip.creatis.insa-lyon.fr_link", bandwidth="10Gbps", latency="500us",
                                         sharing_policy="FULLDUPLEX"))
  vip_host_link <- newXMLNode("host_link", attrs=c(id="vip.creatis.insa-lyon.fr",
                                                   up="vip.creatis.insa-lyon.fr_link_UP",
                                                   down="vip.creatis.insa-lyon.fr_link_DOWN"))
  lfc      <- newXMLNode("host", attrs=c(id="lfc-biomed.in2p3.fr", speed="5Gf", core="48"))
  lfc_link <- newXMLNode("link", attrs=c(id="lfc-biomed.in2p3.fr_link", bandwidth="10Gbps", latency="500us",
                                         sharing_policy="FULLDUPLEX"))
  lfc_host_link <- newXMLNode("host_link", attrs=c(id="lfc-biomed.in2p3.fr",
                                                   up="lfc-biomed.in2p3.fr_link_UP",
                                                   down="lfc-biomed.in2p3.fr_link_DOWN"))
  c(vip, vip_link, vip_host_link, lfc, lfc_link, lfc_host_link) 
}

Worker_in_single_AS <- function(worker){
  host <- newXMLNode("host", attrs=c(id=as.character(worker[1]),
                                     speed=paste0(as.numeric(worker[3]),"Mf"),
                                     core=as.numeric(worker[2])),
                     newXMLNode("prop", attrs=c(id="closeSE", value=as.character(worker[6]))))
  link <- newXMLNode("link", attrs=c(id=paste0(worker[1],"_link"), latency="500us",
                                     bandwidth=paste0(as.numeric(worker[4]),"Mbps"),
                                     sharing_policy="FULLDUPLEX"))
  host_link <- newXMLNode("host_link", attrs=c(id=as.character(worker[1]), up=paste0(worker[1],"_link_UP"),
                                               down=paste0(worker[1],"_link_DOWN")))
  c(host, link, host_link)
}

SEs_in_single_AS <-function(SE, METHOD, SYM){
  id <-as.character(SE[1])
  host <- newXMLNode("host", attrs=c(id=id, speed="5Gf", core="48"))
  host_link <- newXMLNode("host_link", attrs=c(id=id,up=paste0(id,"_link_UP"), down=paste0(id,"_link_DOWN")))
  if (METHOD == "Avg")
    if (SYM == "Sym" || is.na(SE[2]) || is.na(SE[4])) {
      link <- newXMLNode("link", attrs=c(id=paste0(id,"_link"), 
                                         bandwidth=paste0(as.numeric(SE[6]),"bps"), latency="500us",
                                         sharing_policy="FULLDUPLEX"))
      c(host, link, host_link)
    } else {
      link_up <- newXMLNode("link", attrs=c(id=paste0(id,"_link_UP"),
                                            bandwidth=paste0(as.numeric(SE[4]),"bps"), latency="500us"))
      link_down <- newXMLNode("link", attrs=c(id=paste0(id,"_link_DOWN"),
                                              bandwidth=paste0(as.numeric(SE[2]),"bps"), latency="500us"))
      c(host, link_up, link_down, host_link)
    }
  else if (METHOD == "Max"){
    if (SYM == "Sym" || is.na(SE[3]) || is.na(SE[5])) {
      link <- newXMLNode("link", attrs=c(id=paste0(id,"_link"),
                                         bandwidth=paste0(as.numeric(SE[7]),"bps"), latency="500us",
                                         sharing_policy="FULLDUPLEX"))
      c(host, link, host_link)
    } else {
      link_up <- newXMLNode("link", attrs=c(id=paste0(id,"_link_UP"),
                                            bandwidth=paste0(as.numeric(SE[5]),"bps"), latency="500us"))
      link_down <- newXMLNode("link", attrs=c(id=paste0(id,"_link_DOWN"),
                                              bandwidth=paste0(as.numeric(SE[3]),"bps"), latency="500us"))
      c(host, link_up, link_down, host_link)
    }
  } else if (METHOD == "Mock_1G"){
    link <- newXMLNode("link", attrs=c(id=paste0(id,"_link"),
                                       bandwidth=paste0(as.numeric(SE[8]),"bps"), latency="500us",
                                       sharing_policy="FULLDUPLEX"))
    c(host, link, host_link)
  } else {
    link <- newXMLNode("link", attrs=c(id=paste0(id,"_link"),
                                       bandwidth=paste0(as.numeric(SE[9]),"bps"), latency="500us",
                                       sharing_policy="FULLDUPLEX"))
    c(host, link, host_link)
  }
}

Routing_in_single_AS <- function(){
  router <- newXMLNode("router", attrs=c(id=paste0(workflow_name, "_router")))
  backbone <- newXMLNode("backbone", attrs=c(id=paste0(workflow_name, "_backbone"),
                                             bandwidth="100GBps", latency="1500us"))
  c(router, backbone)
}

Service_AS <- function(){
  AS     <- newXMLNode("AS", attrs=c(id="Services", routing="Full"))
  vip    <- newXMLNode("host", attrs=c(id="vip.creatis.insa-lyon.fr", speed="5Gf",core="48"))
  lfc    <- newXMLNode("host", attrs=c(id="lfc-biomed.in2p3.fr", speed="5Gf", core="48"))
  router <- newXMLNode("router", attrs=c(id="Services_router"))
  
  backbone <- newXMLNode("link",attrs=c(id="Services_backbone", bandwidth="100Gbps", latency="750us"))
  vip_link <- newXMLNode("link", attrs=c(id="vip.creatis.insa-lyon.fr_link", bandwidth="10Gbps", latency="500us"))
  lfc_link <- newXMLNode("link", attrs=c(id="lfc-biomed.in2p3.fr_link", bandwidth="10Gbps", latency="500us"))
  
  vip_route <-  newXMLNode("route", attrs=c(src="vip.creatis.insa-lyon.fr", dst="Services_router"), 
                           newXMLNode("link_ctn", attrs=c(id="vip.creatis.insa-lyon.fr_link")), 
                           newXMLNode("link_ctn", attrs=c(id="Services_backbone")))
  lfc_route <-  newXMLNode("route", attrs=c(src="lfc-biomed.in2p3.fr", dst="Services_router"), 
                           newXMLNode("link_ctn", attrs=c(id="lfc-biomed.in2p3.fr_link")), 
                           newXMLNode("link_ctn", attrs=c(id="Services_backbone")))
  addChildren(AS, vip, lfc, router, backbone, vip_link, lfc_link, vip_route, lfc_route)
}

SE_AS <-function(name){
  newXMLNode("AS", attrs=c(id=paste("AS",name,sep="_"), routing="None"), 
             newXMLNode("host", attrs=c(id=name, speed="5Gf", core="48")))
}

#FIXME: to keep?
Intra_link <- function(x){
  newXMLNode("link", attrs= c(id=as.character(x[1]), 
                              bandwidth=paste0(as.numeric(x[2]),"bps"), latency="750us"))
}

#FIXME: to keep?
Site_AS_multi_routers_with_limiters <- function(Site){
  site_name = unique(Site$SiteName)
  
  cluster_routes = select_cluster_routes(Site, storage_elements)
  My_SEs = unique(c(unique(cluster_routes[grep(site_name,cluster_routes$dst),2]), 
                    unique(cluster_routes[grep(site_name,cluster_routes$src),1])))
  
  AS         <- newXMLNode("AS", attrs=c(id=paste0("AS_", site_name), routing="Full"))
  clusters   <- apply(Site, 1, function(c)
    newXMLNode("cluster", attrs=c(id=as.character(c[9]), c[1], c[8], c[2], speed=paste0(c[4],"Mf"), 
                                  core=as.character(c[3]), 
                                  bw=paste0(c[5],"Mbps"), lat="500us", sharing_policy="FATPIPE", 
                                  limiter_link= paste0(2*as.numeric(c[5]),"Mbps"), 
                                  router_id=paste0(as.character(c[9]), "_router")),
               newXMLNode("prop", attrs=c(id="closeSE", value=as.character(c[7])))))
  
  router_AS  <- newXMLNode("AS",  attrs=c(id=paste("AS",site_name,"gw", sep="_"), routing="Full"),
                           newXMLNode("router", attrs=c(id=paste("AS",site_name,"router", sep="_"))))
  backbone   <- newXMLNode("link",attrs=c(id=paste(site_name,"backbone", sep="_"), 
                                          bandwidth="100Gbps", latency="750us"))
  links <- apply(bandwidth_by_clusterlink[grep(site_name, bandwidth_by_clusterlink$ClusterLink),c(1,7)], 
                 1, Intra_link)

  routes     <-  apply(Site, 1, function(c)
    newXMLNode("ASroute", attrs=c(src=as.character(c[9]), dst=paste("AS",site_name, "gw", sep="_"),
                                  gw_src=paste0(as.character(c[9]), "_router"), 
                                  gw_dst=paste("AS",site_name,"router", sep="_")), 
               newXMLNode("link_ctn", attrs=c(id=paste(site_name,"backbone", sep="_")))))
  
  addChildren(AS, clusters, router_AS, backbone,links, routes)
}

Site_AS_with_limiters <- function(df){
  site_name = unique(df$SiteName)
  AS         <- newXMLNode("AS", attrs=c(id=paste0("AS_", site_name), routing="Full"))
  clusters   <- apply(df, 1, function(c)
    newXMLNode("cluster", attrs=c(id=as.character(c[9]), c[1], c[8], c[2], speed=paste0(c[4],"Mf"), 
                                  core=as.character(c[3]), 
                                  bw=paste0(c[5],"Mbps"), lat="500us", sharing_policy="FATPIPE", 
                                  limiter_link= paste0(2*as.numeric(c[5]),"Mbps"), 
                                  router_id=paste0(as.character(c[9]), "_router")),
               newXMLNode("prop", attrs=c(id="closeSE", value=as.character(c[7])))))
  router_AS  <- newXMLNode("AS",  attrs=c(id=paste("AS",site_name,"gw", sep="_"), routing="Full"),
                           newXMLNode("router", attrs=c(id=paste("AS",site_name,"router", sep="_"))))
  backbone   <- newXMLNode("link",attrs=c(id=paste(site_name,"backbone", sep="_"), 
                                          bandwidth="100Gbps", latency="750us"))
  routes     <-  apply(df, 1, function(c)
    newXMLNode("ASroute", attrs=c(src=as.character(c[9]), dst=paste("AS",site_name, "gw", sep="_"),
                                  gw_src=paste0(as.character(c[9]), "_router"), 
                                  gw_dst=paste("AS",site_name,"router", sep="_")), 
               newXMLNode("link_ctn", attrs=c(id=paste(site_name,"backbone", sep="_")))))
  addChildren(AS, clusters, router_AS, backbone, routes)
}

Site_AS_without_limiters <- function(df){
  site_name = unique(df$SiteName)
  AS         <- newXMLNode("AS", attrs=c(id=paste0("AS_", site_name), routing="Full"))
  clusters   <- apply(df, 1, function(c)
    newXMLNode("cluster", attrs=c(id=as.character(c[9]), c[1], c[8], c[2], speed=paste0(c[4],"Mf"), 
                                  core=as.character(c[3]), 
                                  bw=paste0(c[5],"Mbps"), lat="500us",  
                                  router_id=paste0(as.character(c[9]), "_router")),
               newXMLNode("prop", attrs=c(id="closeSE", value=as.character(c[7])))))
  router_AS  <- newXMLNode("AS",  attrs=c(id=paste("AS",site_name,"gw", sep="_"), routing="Full"),
                           newXMLNode("router", attrs=c(id=paste("AS",site_name,"router", sep="_"))))
  backbone   <- newXMLNode("link",attrs=c(id=paste(site_name,"backbone", sep="_"), 
                                          bandwidth="100Gbps", latency="750us"))
  routes     <-  apply(df, 1, function(c)
    newXMLNode("ASroute", attrs=c(src=as.character(c[9]), dst=paste("AS",site_name, "gw", sep="_"),
                                  gw_src=paste0(as.character(c[9]), "_router"), 
                                  gw_dst=paste("AS",site_name,"router", sep="_")), 
               newXMLNode("link_ctn", attrs=c(id=paste(site_name,"backbone", sep="_")))))
  addChildren(AS, clusters, router_AS, backbone, routes)
}

Shared_link <- function(x){
  newXMLNode("link", attrs= c(id=as.character(x[1]), 
                              bandwidth=paste0(as.numeric(x[2]),"bps"), latency="500us"))
}

Fatpipe_link <- function(x){
  newXMLNode("link", attrs= c(id=as.character(x[1]), bandwidth=paste0(as.numeric(x[2]),"bps"),
                              latency="500us",sharing_policy="FATPIPE"))
}

Services_to_site <- function (site_name){
  newXMLNode("ASroute", attrs=c(src="Services", dst=paste("AS", site_name, sep="_"), gw_src="Services_router", 
                                gw_dst=paste("AS",site_name,"router", sep="_")),
             newXMLNode("link_ctn", attrs=c(id="service_link")))
} 

Services_to_SE <- function (SE_name){
  newXMLNode("ASroute", attrs=c(src="Services", dst=paste0("AS_", SE_name), 
                                gw_src="Services_router", gw_dst=SE_name), 
             newXMLNode("link_ctn", attrs=c(id="service_link")))
}

Site_to_from_SE <- function (x){
  route = newXMLNode("ASroute", attrs=c(x[1], x[2], x[5], x[6]), 
                     newXMLNode("link_ctn", attrs=c(id=as.character(x[3]))))
  if (as.character(x[7]) == "NO"){
    addAttributes(route, symmetrical="NO")
  }
  route
}

export_single_AS_XML <- function (METHOD, SYM){
  t = xmlTree("platform", attrs=c(version="4"), 
              dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd"')
  t$addNode("AS", attrs=c(id=workflow_name, routing="Cluster"), 
            .children = c(Service_nodes_in_single_AS(), apply(workers, 1, Worker_in_single_AS), 
                          apply(bandwidth_by_SE, 1, function(x) SEs_in_single_AS(x,METHOD, "SYM")),
                          Routing_in_single_AS()))
  cat(saveXML(t), file=paste0(output_dir,"single_AS_",workflow_name,"_",METHOD,"_",SYM,".xml"))
}  

export_XML <-function(SITES, LINKS, TAG){
  t = xmlTree("platform", attrs=c(version="4"), 
              dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd"')
  t$addNode("AS", attrs=c(id=workflow_name, routing="Full"),
            .children= c(service_AS, SITES, all_SEs, Service_link, LINKS, 
                         Services_to_site_routes, Services_to_SE_routes, Sites_to_from_SE_routes))
  cat(saveXML(t), file=paste0(output_dir,"platform_",workflow_name,"_",TAG,".xml"))
}
############################### Produce data frames and lists ##########################################
workers           <- get_worker_nodes(paste0(wd,'worker_nodes.csv'))
sites             <- unique(workers$SiteName)
local_ses         <- unique(workers$CloseSE)
clusters          <- build_clusters(workers)
transfers         <- get_transfers(paste0(wd,'file_transfer.csv'))
transfers         <- correct_bandwidth(transfers)

bandwidth_by_SE   <- get_bandwidths_by_SE(transfers)
bandwidth_by_link <- get_bandwidths_by_link(transfers)
bandwidth_by_clusterlink <- get_bandwidths_by_clusterlink(transfers)
bandwidth_by_link <- merge(bandwidth_by_link, sort=FALSE,
                           ddply(bandwidth_by_clusterlink, .(Link), summarize, Agg_Corr_Max = max(Corr_Max)))

storage_elements  <- unique(c(transfers[transfers$UpDown != 2,]$Destination, 
                              transfers[transfers$UpDown == 2,]$Source))
routes            <- select_routes(sites, storage_elements)

############################### Produce all the components of the XML files ############################
service_AS <- Service_AS()
all_site_ASes_with_limiters <- dlply(clusters, .(SiteName), Site_AS_with_limiters)
all_site_ASes_without_limiters <- dlply(clusters, .(SiteName), Site_AS_without_limiters)
#all_site_ASes_multi_routers_with_limiters <- dlply(clusters, .(SiteName), Site_AS_multi_routers_with_limiters)
all_SEs <- lapply(storage_elements, SE_AS)

Service_link   <- newXMLNode("link", attrs=c(id="service_link", bandwidth="10Gbps", latency="500us"))
Avg_links      <- apply(bandwidth_by_link[,c(1,4)], 1, Fatpipe_link)
Max_links      <- apply(bandwidth_by_link[,c(1,5)], 1, Shared_link)
Corr_Avg_links <- apply(bandwidth_by_link[,c(1,6)], 1, Shared_link)
Corr_Max_links <- apply(bandwidth_by_link[,c(1,7)], 1, Shared_link)
Mock_1G_links  <- apply(bandwidth_by_link[,c(1,8)], 1, Shared_link)
Mock_10G_links <- apply(bandwidth_by_link[,c(1,9)], 1, Shared_link)
Agg_Corr_Max_link <- apply(bandwidth_by_link[,c(1,10)], 1, Shared_link)

Services_to_site_routes <- lapply(sites, Services_to_site)
Services_to_SE_routes <- lapply(storage_elements, Services_to_SE)
Sites_to_from_SE_routes <- apply(routes, 1, Site_to_from_SE)
############################### Create and export all the XML files ####################################
export_single_AS_XML("Mock_1G", "Sym")
export_single_AS_XML("Mock_10G", "Sym")

export_single_AS_XML("Avg", "Sym")
export_single_AS_XML("Avg", "Asym")
export_single_AS_XML("Max", "Sym")
export_single_AS_XML("Max", "Asym")

export_XML(all_site_ASes_without_limiters, Avg_links, "Avg_no_lim")
export_XML(all_site_ASes_with_limiters, Avg_links, "Avg_lim")

export_XML(all_site_ASes_without_limiters, Max_links, "Max_no_lim")
export_XML(all_site_ASes_with_limiters, Max_links, "Max_lim")

export_XML(all_site_ASes_without_limiters, Corr_Avg_links, "Corr_Avg_no_lim")
export_XML(all_site_ASes_with_limiters, Corr_Avg_links, "Corr_Avg_lim")

export_XML(all_site_ASes_without_limiters, Corr_Max_links, "Corr_Max_no_lim")
export_XML(all_site_ASes_with_limiters, Corr_Max_links, "Corr_Max_lim")

export_XML(all_site_ASes_without_limiters, Mock_1G_links, "Mock_1G_no_lim")
export_XML(all_site_ASes_with_limiters, Mock_1G_links, "Mock_1G_lim")

export_XML(all_site_ASes_without_limiters, Mock_10G_links, "Mock_10G_no_lim")
export_XML(all_site_ASes_with_limiters, Mock_10G_links, "Mock_10G_lim")

export_XML(all_site_ASes_with_limiters, Agg_Corr_Max_link, "ultimate")
