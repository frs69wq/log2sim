#!/usr/bin/Rscript
########################################################################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                                                           #
# Contributor(s) : Frédéric SUTER (2015-2016), Anchen CHAI (2016)                                                      #
#                                                                                                                      #
# This program is free software; you can redistribute it and/or modify it under the terms of the license (GNU LGPL)    #
# which comes with this code.                                                                                          #
########################################################################################################################
################################################## Required R packages #################################################
library(XML)
library(plyr)
################################################## Parsing command line arguments ######################################
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
################################################## Data preparation on worker_nodes.csv ################################
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
################################################## Data preparation on db_dump.csv #####################################
get_job_start_times <- function (file_name){
  df <- subset(read.csv(file_name, header = TRUE, sep=' ',as.is=TRUE),
               select = c(JobId, DownloadStartTime,UploadStartTime))
  df$End <-0
  rename(df,c("DownloadStartTime" = "Start", "UploadStartTime" = "Start_Upload"))
}
################################################## Data preparation on file_transfer.csv ###############################
update_start_end_times <-function(df){
  gate_count <- (nrow(df)-5)/6
  for(j in 1:(nrow(df)-gate_count-5)){
    # Each Gate job corresponds to 5 transfers in the data frame:
    #   - upload test
    #   - upload result
    #   - download of 3 inputs
    if (j %%5 == 2) # Upload of partial result, use StartUpload as start time
      df[j,]$Start = df[j,]$Start_Upload
    if (j %% 5 == 3) # Skip the interleaved upload to get the end time of upload_test as start time of the 1st download
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
  df$Bandwidth  <- (8*df$FileSize)/pmax(0.001,(df$Time-1))

  # Construct the link name:
  # if UpDown == 2 (Download), its Source_SiteName
  # Otherwise (Upload-test or Upload), it's SiteName_Destination
  df$Link <- apply(df[,c(2:3,7:8)], 1, function(x)
    if (x[4]==2) paste0(x[1],'-',x[3]) else paste0(x[3],'-',x[2]))

  # Construct the cluster name
  df$Cluster <- apply(df[,c(2:3,8)],1, function(x)
    if (x[3]==2)  workers[workers$hostname == strsplit(x[2],"[.]")[[1]][1],]$ClusterName
    else          workers[workers$hostname == strsplit(x[1],"[.]")[[1]][1],]$ClusterName)

  # Construct the internal link name:
  # if UpDown == 2 (Download), its Source_SiteName_Cluster
  # Otherwise (Upload-test or Upload), it's Cluster_SiteName_Destination
  df$ClusterLink <- apply(df[,c(2:3,7:8,11)], 1, function(x)
    if (x[4]==2) paste0(x[3],'-',x[1],'-',x[5]) else paste0(x[5],'-',x[3],'-',x[2]))

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
################################################## Maximum bandwidth correction ########################################
correct_bandwidth <-function(df){
  df$concurrency_by_Link <- df$concurrency_by_Cluster <- 1
  df$concurrency_by_SE   <- df$concurrency_by_Site    <- 1
  for(i in 1:nrow(df)){
    # Get the begin and end time of this transfer. Add 1 second (latency) to the start time
    begin <- df[i,]$Start + 1
    end <- df[i,]$End
    to_or_from <- if (df[i,]$UpDown == 2) "from" else "to"

    # Determine the number of intervals in which split the transfer time: At least 1 interval and at most 50
    # Transfers shorter than 5 seconds are split in 10 times their duration
    # Example: a transfer of 2 seconds will be split in 20 intervals
    n_interval <- max(1,min(50,ceiling((10*(end-begin)))))

    # Set the length of each interval
    step <- (end - begin) / n_interval

    # Get the set of concurrent transfers once for all. They have to begin
    # before the end  AND end after the beginning of the current transfer
    if (to_or_from == "from"){
      others_by_SE   <- subset(df, Source == df[i,]$Source & UpDown == 2 & (Start + 1) < end & End >= begin)
      others_by_Site <- subset(df, SiteName == df[i,]$SiteName & UpDown == 2 & (Start + 1) < end & End >= begin)

      others_by_Link <- subset(df, Link == df[i,]$Link & UpDown == 2 & (Start + 1) < end & End >= begin)
      others_by_Cluster <- subset(df, Cluster == df[i,]$Cluster & UpDown == 2 & (Start + 1) < end & End >= begin)
    } else {
      others_by_SE   <- subset(df, Destination == df[i,]$Destination & UpDown != 2 & (Start + 1) < end & End >= begin)
      others_by_Site <- subset(df, SiteName == df[i,]$SiteName & UpDown != 2 & (Start + 1) < end & End >= begin)

      others_by_Link <- subset(df, Link == df[i,]$Link & UpDown != 2 & (Start + 1) < end & End >= begin)
      others_by_Cluster <- subset(df, Cluster == df[i,]$Cluster & UpDown != 2 & (Start + 1) < end & End >= begin)
    }
    if (n_interval == 1){
      # if there is only one interval to cover, we're done
      df[i,]$concurrency_by_SE      <- max(1,nrow(others_by_SE))
      df[i,]$concurrency_by_Site    <- max(1,nrow(others_by_Site))
      df[i,]$concurrency_by_Link    <- max(1,nrow(others_by_Link))
      df[i,]$concurrency_by_Cluster <- max(1,nrow(others_by_Cluster))
    } else {
      cur_concurrency_by_SE <- cur_concurrency_by_Site <- 0
      cur_concurrency_by_Link <- cur_concurrency_by_Cluster <- 0
      if (nrow(others_by_Link) > 1) {
        # This makes sense only if there is concurrency
        for(s in 1: n_interval){
          # set the end of the current interval
          end <- begin + step
          # accumulate the inverse of the number of concurrent transfers over
          # this interval
          cur_concurrency_by_SE <- cur_concurrency_by_SE +
            1/(nrow(subset(others_by_SE, (Start +1) < end & End >= begin)))
          cur_concurrency_by_Site <- cur_concurrency_by_Site +
            1/(nrow(subset(others_by_Site, (Start +1) < end & End >= begin)))
          cur_concurrency_by_Link <- cur_concurrency_by_Link +
            1/(nrow(subset(others_by_Link, (Start +1) < end & End >= begin)))
          cur_concurrency_by_Cluster <- cur_concurrency_by_Cluster +
            1/(nrow(subset(others_by_Cluster, (Start +1) < end & End >= begin)))
          # update begin for the next round
          begin <- end
        }
        # End of formula: N/sum_{i=1}^{N}(1/C_i)
        df[i,]$concurrency_by_SE      <- n_interval / cur_concurrency_by_SE
        df[i,]$concurrency_by_Site    <- n_interval / cur_concurrency_by_Site
        df[i,]$concurrency_by_Link    <- n_interval / cur_concurrency_by_Link
        df[i,]$concurrency_by_Cluster <- n_interval / cur_concurrency_by_Cluster
      }
    }
  }
  df$Corr_Bandwidth_by_SE      <- df$Bandwidth * df$concurrency_by_SE
  df$Corr_Bandwidth_by_Site    <- df$Bandwidth * df$concurrency_by_Site
  df$Corr_Bandwidth_by_Link    <- df$Bandwidth * df$concurrency_by_Link
  df$Corr_Bandwidth_by_Cluster <- df$Bandwidth * df$concurrency_by_Cluster
  df
}
################################################## Bandwidth aggregation methods #######################################
get_bandwidths_by_Site <- function(Transfers){
  subset(ddply(Transfers, c("SiteName", "File_Type"), summarize, Default="100Gbps",
               Max=paste0(round(max(Bandwidth)/.97),"bps"),
               Corr_Max=paste0(round(max(Corr_Bandwidth_by_Site)/.97),"bps")),
         File_Type == 'Release', select=c("SiteName", "Default", "Max", "Corr_Max"))
}

get_bandwidths_by_SE <- function(Transfers){
  to_SE <- ddply(Transfers[Transfers$UpDown != 2,], c("Destination"), summarize,
              Avg=round(mean(Bandwidth)), Max=round(max(Bandwidth)))
  names(to_SE) = c("SE","Avg_to","Max_to")
  from_SE <- ddply(Transfers[Transfers$UpDown == 2,], c("Source"), summarize,
                   Avg=round(mean(Bandwidth)), Max=round(max(Bandwidth)))
  names(from_SE) = c("SE","Avg_from","Max_from")
  df <- merge(to_SE, from_SE, all=TRUE)
  df$Avg <- apply(df[,c(2,4)], 1, function(s) if (is.na(s[1])) s[2] else if (is.na(s[2])) s[1] else (s[1]+s[2])/2)
  df$Max <- apply(df[,c(3,5)], 1, function(s) if (is.na(s[1])) s[2] else if (is.na(s[2])) s[1] else (max(s[1], s[2])))
  df$Mock_10G =1e10
  subset(df, select=c("SE", "Avg", "Max", "Mock_10G"))
}

get_bandwidths_by_SE_and_type <- function(Transfers){
  to_SE <- ddply(Transfers[Transfers$UpDown != 2,], c("Destination", "File_Type"), summarize,
                 Avg=round(mean(Bandwidth)), Max=round(max(Bandwidth)), Corr_Max = round(max(Corr_Bandwidth_by_SE)))
  names(to_SE) = c("SE","File_Type","Avg_to","Max_to", "Corr_Max_to")
  upload <- subset(to_SE, File_Type == 'Partial Upload')
  others <- ddply(subset(to_SE, !(SE %in% upload$SE)), .(SE), function(x) x[which.max(x$Max_to),])
  to_SE <- rbind(upload,others)
  from_SE <- ddply(Transfers[Transfers$UpDown == 2,], c("Source","File_Type"), summarize,
                   Avg=round(mean(Bandwidth)), Max=round(max(Bandwidth)), Corr_Max = round(max(Corr_Bandwidth_by_SE)))
  names(from_SE) = c("SE","File_Type","Avg_from","Max_from", "Corr_Max_from")
  release <- subset(from_SE, File_Type == c('Release'))
  others <- ddply(subset(from_SE, !(SE %in% release$SE)), .(SE), function(x) x[which.max(x$Max_from),])
  from_SE <- rbind(release,others)
  df <- merge(to_SE, from_SE, by=c("SE","File_Type"), all=TRUE)
  df <- suppressWarnings(aggregate(df,by=list(x=df$SE), min, na.rm=TRUE))
  subset(df, select=-c(x,File_Type))
}

get_bandwidths_by_Link <- function(Transfers){
  df <- ddply(Transfers, c("Link","File_Type"), summarize, count=length(Link), Avg=round(mean(Bandwidth)),
              Max=round(max(Bandwidth)),Corr_Max=round(max(Corr_Bandwidth_by_Link)))
  release <- subset(df, File_Type == 'Release')
  others <- ddply(subset(df, !(Link %in% release$Link)), .(Link), function(x) x[which.max(x$Max),])
  rbind(release,others)
}

get_bandwidths_by_Cluster <- function(Transfers){
  df <- ddply(Transfers, c("Cluster","File_Type"), summarize, count=length(Cluster),
              Avg=round(mean(Bandwidth)), Max=round(max(Bandwidth)),
              Corr_Max=round(max(Corr_Bandwidth_by_Cluster)))
  release <- subset(df, File_Type == 'Release')
  others <- ddply(subset(df, !(Cluster %in% release$Cluster)), .(Cluster), function(x) x[which.max(x$Max),])
  subset(rbind(release,others), select=-c(File_Type, count))
}
################################################## Route selection #####################################################
select_shared_routes <- function(Sites, SEs){
  df <- merge(SEs, Sites)
  names(df) <- c("src", "dst")
  df <- subset (df, paste0(src,"-",dst) %in% bandwidth_by_Link$Link |paste0(dst,"-",src) %in% bandwidth_by_Link$Link)
  df$Link <- paste0(df$src,"_link")
  df$dst <- paste0("AS_", df$dst)
  df$gw_src <- df$src
  df$src <- paste0("AS_", df$src)
  df$gw_dst <- paste0(df$dst, "_router")
  df
}

select_asymmetric_shared_routes <- function(Sites, SEs){
  to_SE <- merge(Sites, SEs)
  names(to_SE) <- c("src", "dst")
  to_SE$Link <- paste0(to_SE$dst,"_link_to")
  to_SE$ReverseLink <- paste0(to_SE$dst, "_link_from")
  to_SE <- subset (to_SE, paste0(src,"-",dst) %in% bandwidth_by_Link$Link)
  to_SE$src <- paste0("AS_", to_SE$src)
  to_SE$gw_src <- paste0(to_SE$src, "_router")
  to_SE$gw_dst <- to_SE$dst
  to_SE$dst <- paste0("AS_", to_SE$dst)

  from_SE <- merge(SEs, Sites)
  names(from_SE) <- c("src", "dst")
  from_SE$Link <- paste0(from_SE$src,"_link_from")
  from_SE$ReverseLink <- paste0(from_SE$src,"_link_to")
  from_SE <- subset (from_SE, paste0(src,"-",dst) %in% bandwidth_by_Link$Link)
  from_SE$dst <- paste0("AS_", from_SE$dst)
  from_SE$gw_src <- from_SE$src
  from_SE$src <- paste0("AS_", from_SE$src)
  from_SE$gw_dst <- paste0(from_SE$dst, "_router")

  to_SE$symmetrical <- apply(to_SE[,c(1,2)], 1,function(x)
    if (nrow(from_SE[from_SE$dst == x[1] & from_SE$src == x[2],])>0) "NO" else "YES")
  from_SE$symmetrical <- apply(from_SE[,c(1,2)], 1,function(x)
    if (nrow(to_SE[to_SE$dst == x[1] & to_SE$src == x[2],])>0) "NO" else "YES")

  rbind(to_SE, from_SE)
}

select_routes <- function(Sites, SEs){
  site_to_SE <- merge(Sites, SEs)
  names(site_to_SE) <- c("src", "dst")
  site_to_SE$Link <- paste(site_to_SE$src, site_to_SE$dst, sep='-')
  site_to_SE$ReverseLink <- paste(site_to_SE$dst, site_to_SE$src, sep='-')
  site_to_SE$Limiter <- paste0(site_to_SE$dst,"_link_to")
  site_to_SE$src <- paste0("AS_", site_to_SE$src)
  site_to_SE$gw_src <- paste0(site_to_SE$src, "_router")
  site_to_SE$gw_dst <- site_to_SE$dst
  site_to_SE$dst <- paste0("AS_", site_to_SE$dst)
  site_to_SE <- subset (site_to_SE, Link %in% bandwidth_by_Link$Link)

  SE_to_site <- merge(SEs, Sites)
  names(SE_to_site) <- c("src", "dst")
  SE_to_site$Link <- paste(SE_to_site$src, SE_to_site$dst, sep='-')
  SE_to_site$ReverseLink <- paste(SE_to_site$dst, SE_to_site$src, sep='-')
  SE_to_site$Limiter <- paste0(SE_to_site$src,"_link_from")
  SE_to_site <- subset (SE_to_site, Link %in% bandwidth_by_Link$Link)
  SE_to_site$dst <- paste0("AS_", SE_to_site$dst)
  SE_to_site$gw_src <- SE_to_site$src
  SE_to_site$src <- paste0("AS_", SE_to_site$src)
  SE_to_site$gw_dst <- paste0(SE_to_site$dst, "_router")

  site_to_SE$symmetrical <- sapply(site_to_SE$ReverseLink, function(x) if (x %in% SE_to_site$Link) "NO" else "YES")
  SE_to_site$symmetrical <- sapply(SE_to_site$ReverseLink, function(x) if (x %in% site_to_SE$Link) "NO" else "YES")

  subset(rbind(site_to_SE, SE_to_site), select = c(1:4,6:8,5))
}
################################################## Generation of the different parts of an XML file ####################
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

Site_AS <- function(df, bb_bw){
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
  backbone <- newXMLNode("link",attrs=c(id=paste(site_name,"backbone", sep="_"),
                                        bandwidth=bandwidth_by_Site[bandwidth_by_Site$SiteName==site_name,bb_bw],
                                        latency="750us"))
  routes     <-  apply(df, 1, function(c)
    newXMLNode("ASroute", attrs=c(src=as.character(c[9]), dst=paste("AS",site_name, "gw", sep="_"),
                                  gw_src=paste0(as.character(c[9]), "_router"),
                                  gw_dst=paste("AS",site_name,"router", sep="_")),
               newXMLNode("link_ctn", attrs=c(id=paste(site_name,"backbone", sep="_")))))
  addChildren(AS, clusters, router_AS, backbone, routes)
}

Site_AS_with_cluster_links <- function(df, bb_bw, c_bw){
  site_name = unique(df$SiteName)
  AS         <- newXMLNode("AS", attrs=c(id=paste0("AS_", site_name), routing="Full"))
  clusters   <- apply(df, 1, function(c)
    newXMLNode("cluster", attrs=c(id=as.character(c[1]), c[2], c[9], c[3], speed=paste0(c[5],"Mf"),
                                  core=as.character(c[4]),
                                  bw=paste0(c[6],"Mbps"), lat="500us", sharing_policy="FATPIPE",
                                  limiter_link= paste0(2*as.numeric(c[6]),"Mbps"),
                                  router_id=paste0(as.character(c[1]), "_router")),
               newXMLNode("prop", attrs=c(id="closeSE", value=as.character(c[8])))))
  router_AS  <- newXMLNode("AS",  attrs=c(id=paste("AS",site_name,"gw", sep="_"), routing="Full"),
                           newXMLNode("router", attrs=c(id=paste("AS",site_name,"router", sep="_"))))
  backbone <- newXMLNode("link",attrs=c(id=paste(site_name,"backbone", sep="_"),
                                        bandwidth=bandwidth_by_Site[bandwidth_by_Site$SiteName==site_name,bb_bw],
                                        latency="0"))
  cluster_links   <- apply(df, 1, function(c){
    link <- newXMLNode("link",attrs=c(id=paste0(c[1],"_link"),
                              bandwidth=paste0(as.numeric(c[c_bw])/0.97,"bps"), latency="750us"))
    if (c_bw == 10) # Avg bandwidth
      addAttributes(link, sharing_policy="FATPIPE")
    link
    })
  routes     <-  apply(df, 1, function(c)
    newXMLNode("ASroute", attrs=c(src=as.character(c[1]), dst=paste("AS",site_name, "gw", sep="_"),
                                  gw_src=paste0(c[1], "_router"),
                                  gw_dst=paste("AS",site_name,"router", sep="_")),
               newXMLNode("link_ctn", attrs=c(id=paste(c[1],"link", sep="_"))),
               newXMLNode("link_ctn", attrs=c(id=paste(site_name,"backbone", sep="_")))))
  addChildren(AS, clusters, router_AS, backbone, cluster_links, routes)
}

Shared_link <- function(x){
  newXMLNode("link", attrs= c(id=as.character(x[1]), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"), latency="500us"))
}

Fatpipe_link <- function(x){
  newXMLNode("link", attrs= c(id=as.character(x[1]), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                              latency="500us",sharing_policy="FATPIPE"))
}

Services_to_site <- function (site_name){
  newXMLNode("ASroute", attrs=c(src="Services", dst=paste("AS", site_name, sep="_"), gw_src="Services_router",
                                gw_dst=paste("AS",site_name,"router", sep="_")),
             newXMLNode("link_ctn", attrs=c(id="service_link")))
}

Services_to_SE <- function (SE_name){
  newXMLNode("ASroute", attrs=c(src="Services", dst=paste0("AS_", SE_name), gw_src="Services_router", gw_dst=SE_name),
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

Site_to_from_SE_limited <- function (x){
  route = newXMLNode("ASroute", attrs=c(x[1], x[2], x[5], x[6]),
                     newXMLNode("link_ctn", attrs=c(id=as.character(x[3]))),
                     newXMLNode("link_ctn", attrs=c(id=as.character(x[8]))))
  if (as.character(x[7]) == "NO"){
    addAttributes(route, symmetrical="NO")
  }
  route
}

Site_to_from_SE_shared <- function (x){
  newXMLNode("ASroute", attrs=c(x[1], x[2], x[4], x[5]), newXMLNode("link_ctn", attrs=c(id=as.character(x[3]))))
}

export_XML <-function(SITES, LINKS, ROUTES, TAG){
  t = xmlTree("platform", attrs=c(version="4"),
              dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd"')
  t$addNode("AS", attrs=c(id=workflow_name, routing="Full"),
            .children= c(service_AS, SITES, all_SEs, Service_link, LINKS,
                         Services_to_site_routes, Services_to_SE_routes, ROUTES))
  cat(saveXML(t), file=paste0(output_dir,"platform_",workflow_name,"_",TAG,".xml"))
}
################################################## Produce data frames and lists #######################################
workers           <- get_worker_nodes(paste0(wd,'worker_nodes.csv'))
sites             <- unique(workers$SiteName)
local_ses         <- unique(workers$CloseSE)
clusters          <- build_clusters(workers)
transfers         <- get_transfers(paste0(wd,'file_transfer.csv'))
transfers         <- correct_bandwidth(transfers)

bandwidth_by_SE          <- get_bandwidths_by_SE(transfers)
bandwidth_by_Site        <- get_bandwidths_by_Site(transfers)
bandwidth_by_SE_and_type <- get_bandwidths_by_SE_and_type(transfers)
bandwidth_by_Link        <- get_bandwidths_by_Link(transfers)
bandwidth_by_Cluster     <- get_bandwidths_by_Cluster(transfers)

storage_elements  <- unique(c(transfers[transfers$UpDown != 2,]$Destination, transfers[transfers$UpDown == 2,]$Source))

shared_routes      <- select_shared_routes(sites, storage_elements)
asym_shared_routes <- select_asymmetric_shared_routes(sites, storage_elements)
routes             <- select_routes(sites, storage_elements)
################################################## Produce all the components of the XML files #########################
service_AS <- Service_AS()

all_site_ASes              <- dlply(clusters, .(SiteName), function(x) Site_AS(x,2))
all_site_ASes_max_lim      <- dlply(clusters, .(SiteName), function(x) Site_AS(x,3))
all_site_ASes_corr_max_lim <- dlply(clusters, .(SiteName), function(x) Site_AS(x,4))
all_site_ASes_with_avg_cluster_links <- dlply(merge(clusters, bandwidth_by_Cluster, by.x="name", by.y="Cluster"),
                                              .(SiteName), function(x) Site_AS_with_cluster_links(x, 2, 10))
all_site_ASes_with_corr_max_cluster_links <- dlply(merge(clusters, bandwidth_by_Cluster, by.x="name", by.y="Cluster"),
                                                   .(SiteName), function(x) Site_AS_with_cluster_links(x, 4, 12))

all_SEs <- lapply(storage_elements, SE_AS)

Service_link          <- newXMLNode("link", attrs=c(id="service_link", bandwidth="10Gbps", latency="500us"))

Mock_10G_shared_links <- apply(bandwidth_by_SE[,c(1,4)], 1, function(x){
  newXMLNode("link", attrs= c(id=paste0(x[1],"_link"), bandwidth=paste0(as.numeric(x[2]),"bps"), latency="750us"))})

Avg_shared_links <- apply(bandwidth_by_SE[,c(1,2)], 1, function(x){
  newXMLNode("link", attrs= c(id=paste0(x[1],"_link"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                              latency="750us", sharing_policy = "FATPIPE"))})

Max_shared_links <- apply(bandwidth_by_SE[,c(1,3)], 1, function(x){
  newXMLNode("link", attrs= c(id=paste0(x[1],"_link"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                              latency="750us"))})

Asym_Avg_shared_links <-
  c(apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Avg_to)),c(1,2)], 1, function(x){
    newXMLNode("link", attrs= c(id=paste0(x[1],"_link_to"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                latency="750us", sharing_policy = "FATPIPE"))}),
    apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Avg_from)),c(1,5)], 1, function(x){
      newXMLNode("link", attrs= c(id=paste0(x[1],"_link_from"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                  latency="750us", sharing_policy = "FATPIPE"))}))

Asym_Max_shared_links <-
  c(apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Max_to)),c(1,3)], 1, function(x){
    newXMLNode("link", attrs= c(id=paste0(x[1],"_link_to"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                latency="750us"))}),
    apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Max_from)),c(1,6)], 1, function(x){
      newXMLNode("link", attrs= c(id=paste0(x[1],"_link_from"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                  latency="750us"))}))
SE_Max_limiters <-
  c(apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Max_to)),c(1,3)], 1, function(x){
    newXMLNode("link", attrs= c(id=paste0(x[1],"_link_to"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                latency="0"))}),
    apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Max_from)),c(1,6)], 1, function(x){
      newXMLNode("link", attrs= c(id=paste0(x[1],"_link_from"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                  latency="0"))}))

SE_Corr_Max_limiters <-
  c(apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Corr_Max_to)),c(1,4)], 1, function(x){
    newXMLNode("link", attrs= c(id=paste0(x[1],"_link_to"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                latency="0"))}),
    apply(bandwidth_by_SE_and_type[!(is.infinite(bandwidth_by_SE_and_type$Corr_Max_from)),c(1,7)], 1, function(x){
      newXMLNode("link", attrs= c(id=paste0(x[1],"_link_from"), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"),
                                  latency="0"))}))

Avg_links             <- apply(bandwidth_by_Link[,c(1,4)], 1, Fatpipe_link)
Max_links             <- apply(bandwidth_by_Link[,c(1,5)], 1, Shared_link)
Corr_Max_links        <- apply(bandwidth_by_Link[,c(1,6)], 1, Shared_link)

Services_to_site_routes           <- lapply(sites, Services_to_site)
Services_to_SE_routes             <- lapply(storage_elements, Services_to_SE)
Shared_routes_to_from_SE          <- apply(shared_routes, 1, Site_to_from_SE_shared)
Asym_shared_routes_to_from_SE     <- apply(asym_shared_routes, 1, Site_to_from_SE)
Sites_to_from_SE_routes           <- apply(routes, 1, Site_to_from_SE)
Limited_routes_to_from_SE         <- apply(routes, 1, Site_to_from_SE_limited)
################################################## Create and export all the XML files #################################
export_XML(all_site_ASes, Mock_10G_shared_links, Shared_routes_to_from_SE,"10G_SE")                    # Sec III.A

export_XML(all_site_ASes, Avg_shared_links, Shared_routes_to_from_SE,"Avg_SE")                         # Sec III.B
export_XML(all_site_ASes, Max_shared_links, Shared_routes_to_from_SE,"Max_SE")                         # Sec III.B

export_XML(all_site_ASes, Asym_Avg_shared_links, Asym_shared_routes_to_from_SE, "Asym_Avg_SE")         # Sec III.C
export_XML(all_site_ASes, Asym_Max_shared_links, Asym_shared_routes_to_from_SE, "Asym_Max_SE")         # Sec III.C

export_XML(all_site_ASes, Avg_links, Sites_to_from_SE_routes,"Avg_Site")                               # Sec III.D
export_XML(all_site_ASes_max_lim, c(SE_Max_limiters, Max_links), Limited_routes_to_from_SE,"Max_Site") # Sec III.D

export_XML(all_site_ASes_corr_max_lim, c(SE_Corr_Max_limiters, Corr_Max_links),                        # Sec III.E
           Limited_routes_to_from_SE,"Corr_Max_Site")

export_XML(all_site_ASes_with_avg_cluster_links, Avg_links, Sites_to_from_SE_routes,"Avg_cluster")     # Sec III.F
export_XML(all_site_ASes_with_corr_max_cluster_links, c(SE_Corr_Max_limiters, Corr_Max_links),         # Sec III.F
           Limited_routes_to_from_SE, "Corr_Max_cluster")
################################################## Deprecated Stuff ####################################################
# Intra_link <- function(x){
#   newXMLNode("link", attrs= c(id=as.character(x[1]), bandwidth=paste0(round(as.numeric(x[2])/.97),"bps"), latency="750us"))
# }
#

# get_bandwidths_by_clusterlink <- function(Transfers){
#   df <- ddply(Transfers, c("ClusterLink","File_Type"), summarize, count=length(ClusterLink),
#               Avg=round(mean(Bandwidth)), Max=round(max(Bandwidth)),
#               Corr_Max=round(max(Corr_Bandwidth_by_ClusterLink)), Mock_1G=1e9, Mock_10G=1e10, Link=unique(Link))
#   release <- subset(df, File_Type == 'Release')
#   others <- ddply(subset(df, !(ClusterLink %in% release$ClusterLink)), .(ClusterLink), function(x) x[which.max(x$Max),])
#   rbind(release,others)
# }

# select_bypass_routes <- function(Clusters){
#   site_name <- unique (Clusters$SiteName)
#   cluster_names <- Clusters$name
#
#   cluster_to_SE <- merge(cluster_names, storage_elements)
#   names(cluster_to_SE) <- c("src", "dst")
#   cluster_to_SE$Link <- paste(cluster_to_SE$src, site_name,cluster_to_SE$dst, sep='-')
#   cluster_to_SE$ReverseLink <- paste(site_name, cluster_to_SE$dst, cluster_to_SE$src, sep='-')
#   cluster_to_SE$SiteLink <- paste(site_name,cluster_to_SE$dst, sep='-')
#   cluster_to_SE$ReverseSiteLink <- paste(cluster_to_SE$dst, site_name, sep='-')
#   cluster_to_SE$src <- as.character(cluster_to_SE$src)
#   cluster_to_SE$dst <- as.character(cluster_to_SE$dst)
#   cluster_to_SE$gw_src <- paste0(cluster_to_SE$src, "_router")
#   cluster_to_SE$gw_dst <- cluster_to_SE$dst
#   cluster_to_SE <- subset (cluster_to_SE, Link %in% bandwidth_by_clusterlink$ClusterLink)
#
#   SE_to_cluster <- merge(storage_elements, cluster_names)
#   names(SE_to_cluster) <- c("src", "dst")
#   SE_to_cluster$Link <- paste(site_name,SE_to_cluster$src, SE_to_cluster$dst, sep='-')
#   SE_to_cluster$ReverseLink <- paste(SE_to_cluster$dst, site_name, SE_to_cluster$src, sep='-')
#   SE_to_cluster$SiteLink <- paste(SE_to_cluster$src, site_name, sep='-')
#   SE_to_cluster$ReverseSiteLink <- paste(site_name, SE_to_cluster$src, sep='-')
#   SE_to_cluster <- subset (SE_to_cluster, Link %in% bandwidth_by_clusterlink$ClusterLink)
#   SE_to_cluster$src <- as.character(SE_to_cluster$src)
#   SE_to_cluster$dst <- as.character(SE_to_cluster$dst)
#   SE_to_cluster$gw_src <- SE_to_cluster$src
#   SE_to_cluster$gw_dst <- paste0(SE_to_cluster$dst, "_router")
#
#   cluster_to_SE$symmetrical <- sapply(cluster_to_SE$ReverseLink, function(x)
#     if (x %in% SE_to_cluster$Link) "NO" else "YES")
#   SE_to_cluster$symmetrical <- sapply(SE_to_cluster$ReverseLink, function(x)
#     if (x %in% cluster_to_SE$Link) "NO" else "YES")
#
#   rbind(cluster_to_SE, SE_to_cluster)
# }

# Bypass_Cluster_to_from_SE <-function (x){
#   route = newXMLNode("bypassASroute", attrs=c(x[2], x[3], x[8], x[9]),
#                      newXMLNode("link_ctn", attrs=c(id=as.character(x[4]))),
#                      newXMLNode("link_ctn", attrs=c(id=as.character(x[6]))))
#   if (as.character(x[7]) == "NO"){
#     addAttributes(route, symmetrical="NO")
#   }
#   route
# }

# Site_AS_without_limiters <- function(df){
#   site_name = unique(df$SiteName)
#   AS         <- newXMLNode("AS", attrs=c(id=paste0("AS_", site_name), routing="Full"))
#   clusters   <- apply(df, 1, function(c)
#     newXMLNode("cluster", attrs=c(id=as.character(c[9]), c[1], c[8], c[2], speed=paste0(c[4],"Mf"),
#                                   core=as.character(c[3]),
#                                   bw=paste0(c[5],"Mbps"), lat="500us",
#                                   router_id=paste0(as.character(c[9]), "_router")),
#                newXMLNode("prop", attrs=c(id="closeSE", value=as.character(c[7])))))
#   router_AS  <- newXMLNode("AS",  attrs=c(id=paste("AS",site_name,"gw", sep="_"), routing="Full"),
#                            newXMLNode("router", attrs=c(id=paste("AS",site_name,"router", sep="_"))))
#   backbone   <- newXMLNode("link",attrs=c(id=paste(site_name,"backbone", sep="_"),
#                                           bandwidth="100Gbps", latency="750us"))
#   routes     <-  apply(df, 1, function(c)
#     newXMLNode("ASroute", attrs=c(src=as.character(c[9]), dst=paste("AS",site_name, "gw", sep="_"),
#                                   gw_src=paste0(as.character(c[9]), "_router"),
#                                   gw_dst=paste("AS",site_name,"router", sep="_")),
#                newXMLNode("link_ctn", attrs=c(id=paste(site_name,"backbone", sep="_")))))
#   addChildren(AS, clusters, router_AS, backbone, routes)
# }

# export_ultimate_XML <- function(SITES, LINKS, CLUSTERLINKS){
#   t = xmlTree("platform", attrs=c(version="4"),
#               dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd"')
#   t$addNode("AS", attrs=c(id=workflow_name, routing="Full"),
#             .children= c(service_AS, SITES, all_SEs, Service_link, LINKS, CLUSTERLINKS,
#                          Services_to_site_routes, Services_to_SE_routes, Sites_to_from_SE_routes,
#                          Clusters_to_from_SE_bypass_routes))
#   cat(saveXML(t), file=paste0(output_dir,"platform_",workflow_name,"_ultimate.xml"))
# }

#bandwidth_by_Link        <- merge(bandwidth_by_Link, sort=FALSE,
#                                  ddply(bandwidth_by_clusterlink, .(Link), summarize, Agg_Corr_Max = max(Corr_Max)))
#bandwidth_by_clusterlink <- get_bandwidths_by_clusterlink(transfers)
#bypass_routes      <- ddply(clusters, .(SiteName), select_bypass_routes)

#all_site_ASes_without_limiters <- dlply(clusters, .(SiteName), Site_AS_without_limiters)
#Mock_1G_links         <- apply(bandwidth_by_Link[,c(1,7)], 1, Shared_link)
#Cluster_Corr_Max_link <- apply(bandwidth_by_Cluster[,c(1,6)], 1, Intra_link)
#Agg_Corr_Max_link     <- apply(bandwidth_by_Link[,c(1,7)], 1, Shared_link)

#Clusters_to_from_SE_bypass_routes <- apply(bypass_routes, 1, Bypass_Cluster_to_from_SE)

#export_XML(all_site_ASes_without_limiters, Mock_1G_links, "Mock_1G_no_lim")
#export_XML(all_site_ASes, Mock_1G_links, "Mock_1G_lim")

#export_XML(all_site_ASes_without_limiters, Mock_10G_links, "Mock_10G_no_lim")
# export_XML(all_site_ASes_without_limiters, Avg_links, "Avg_no_lim")
# export_XML(all_site_ASes_without_limiters, Max_links, "Max_no_lim")
# export_XML(all_site_ASes_without_limiters, Corr_Max_links, "Corr_Max_no_lim")
#export_XML(all_site_ASes, Mock_10G_links, Sites_to_from_SE_routes, "Mock_10G_lim")             # Unused
#export_ultimate_XML(all_site_ASes, Agg_Corr_Max_link, Cluster_Corr_Max_link)
