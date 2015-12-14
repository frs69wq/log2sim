#!/usr/bin/Rscript

#### Required R packages
library(XML)
library(plyr)

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
workers <- read.csv(paste(wd,'worker_nodes.csv', sep="/"), header=TRUE, 
                    sep=',', as.is=TRUE)

# Get information about file transfers 
transfers <- read.csv(paste(wd,'file_transfer.csv', sep="/"), header = TRUE, 
                      sep=',')

# Remove the upload-tests and the small downloads of 11 bytes made by merge
transfers=transfers[transfers$FileSize >12,]

# Compute the observed bandwidth for each individual file transfer
# remove 990msec from the transfer time (dispatched as network/control latency)
transfers$Bandwidth <- transfers$FileSize/(transfers$Time-990)

# Store the list of identified grid sites and local SEs
sites <- unique(workers$SiteName)
local_ses <- unique(workers$CloseSE)

# Check for anormalities in transfers:
#   * some SEs are used for upload without being declared as local
# If true, this indicates a problem in the logs, hence the generation has
# to be stopped
if (length(unique(transfers[!transfers$Destination %in% local_ses &
                              transfers$UpDown==1,]$Destination))>0){
  stop("Some SEs are used for upload without being declared as local.")
}

# Identify Sources of input files that are not located in one the already
# declared grid site. This means that these SE are used for input downloads
# only.
non_local_input_downloads <- transfers[!transfers$Source %in% local_ses &
                                         transfers$UpDown == 2,]

non_local_input_ses <- unique(as.character(non_local_input_downloads$Source))

# Compute the respective average bandwidth from these SE to each grid site
# Let ddply produce NaN entries. The rationale is that during the simulation
# the LFC can pick a SE for download input that was not selected during the real
# execution. To circumvent this, we add a default bandwidth for the missing 
# connections. This value is set to the maximum observed bandwidth.

input_SE_to_site_bw = ddply(non_local_input_downloads,
                            c("Source","SiteName"),summarize, 
                            AvgBandwidth=round(mean(Bandwidth),2), .drop=FALSE)
input_SE_to_site_bw = input_SE_to_site_bw[! input_SE_to_site_bw$Source %in%
                                            c(workers$Name,local_ses) ,]

input_SE_to_site_bw[is.nan(input_SE_to_site_bw$AvgBandwidth),]$AvgBandwidth <- 
  max(transfers$Bandwidth)

# Identify all the local SEs from which the merge job has to download
# partial results
# First discard the first two downloads (release + inputs)
downloads_merge <-tail(transfers[transfers$JobType=="merge" & 
                                   transfers$UpDown==2,], -2)
# Then identify the host running the merge job 
worker_merge <- unique(downloads_merge$Destination)
# and its associate SE
se_merge = workers[workers$Name==worker_merge,8]

# For all SEs but 'se_merge', compute the average bandwidth to the merge site
ext_downloads_merge = downloads_merge[downloads_merge$Source != se_merge,]
localSE_to_merge = ddply(ext_downloads_merge,c("Source","SiteName"),summarize, 
                         AvgBandwidth=round(mean(Bandwidth),2))

# Define a dataframe to know, for each AS
#    * the name of its storage element
#    * its routing method
AS_se <- data.frame(AS=character(0),NameSE=character(0), Routing=character(0))

#### Generation of the XML tree

# Creation and header
t = xmlTree("platform", attrs=c(version="3"), 
            dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid.dtd"')
t$addTag("AS", attrs=c(id=workflow_name, routing="Full"), close=FALSE)

# Definition of a first AS called 'Services' that comprises
#     * a router
#     * a backbone
#     * the master server 'vip.creatis.insa-lyon.fr'
#     * the logical file catalog 'lfc-biomed.in2p3.fr'

t$addTag("AS", attrs=c(id="Services", routing="Cluster"), close=FALSE)
t$addTag("router", attrs=c(id="Services_router"))
t$addTag("backbone",attrs=c(id="Services_backbone", bandwidth="100GBps", 
                            latency="1500us"))

t$addTag("host", attrs=c(id="vip.creatis.insa-lyon.fr", power="5Gf",core="4"))
t$addTag("link", attrs=c(id="vip.creatis.insa-lyon.fr_link", bandwidth="10Gbps", 
                         latency="500us", sharing_policy="FULLDUPLEX"))
t$addTag("host_link", attrs=c(id="vip.creatis.insa-lyon.fr", 
                              up="vip.creatis.insa-lyon.fr_link_UP",
                              down="vip.creatis.insa-lyon.fr_link_DOWN"))
  
t$addTag("host", attrs=c(id="lfc-biomed.in2p3.fr", power="5Gf", core="4"))
t$addTag("link", attrs=c(id="lfc-biomed.in2p3.fr_link", bandwidth="10Gbps", 
                         latency="500us", sharing_policy="FULLDUPLEX"))
t$addTag("host_link", attrs=c(id="lfc-biomed.in2p3.fr", 
                              up="lfc-biomed.in2p3.fr_link_UP",
                              down="lfc-biomed.in2p3.fr_link_DOWN"))

# t$addTag("host", attrs=c(id="ccsrm02.in2p3.fr", power="5Gf"))
# t$addTag("link", attrs=c(id="ccsrm02.in2p3.fr_link",
#                          bandwidth="10368.1kBps",
#                          latency="500us",
#                          sharing_policy="FULLDUPLEX"))
# t$addTag("host_link", id="ccsrm02.in2p3.fr", up="ccsrm02.in2p3.fr_link_UP", 
#          down="ccsrm02.in2p3.fr_link_DOWN"))
  
t$closeTag()


for (i in sites){
  # Definition of an AS for each identified grid site that comprises
  #     * a router
  #     * a backbone
  #     * all the used worker nodes that belong to this site
  #     * the local SE declared by the worker nodes (this SE has to be unique)
  # The routing method for this AS is 'Cluster'

  t$addTag("AS", attrs=c(id=paste("AS",i, sep="_"), routing="Cluster"), 
           close=FALSE)
  t$addTag("router", attrs=c(id=paste("AS",i,"router", sep="_")))
  t$addTag("backbone",attrs=c(id=paste(i,"backbone", sep="_"), 
                              bandwidth="100GBps", latency="1500us"))
  
  w = workers[workers$SiteName == i,]
  for (j in 1:nrow(w)){
    # Declaration of the host and its close SE
    t$addTag("host", attrs=c(id=w[j,2], power=w[j,4], core=w[j,3]), close=FALSE)
    t$addTag("prop", attrs=c(id="closeSE", value=w[j,8]))
    t$closeTag()
    
    # Declaration of the full-duplex link that connects the host to the AS's 
    # backbone
    t$addTag("link", attrs=c(id=paste(w[j,2],"link",sep="_"), bandwidth=w[j,5],
                             latency="500us", sharing_policy="FULLDUPLEX"))
    t$addTag("host_link", attrs=c(id=w[j,2],
                                  up=paste(w[j,2],"link_UP",sep="_"), 
                                  down=paste(w[j,2],"link_DOWN",sep="_")))  
  }
  
  # Check if more than one SE has been declared as local by worker nodes
  if (length(unique(as.factor(w$closeSE)))>1){
    # If yes, stop the generation, this situation is not handled yet 
    stop("Worker nodes of a same site declare different local SEs")
  } else{
    # Registe local SE and routing method for this site
    new_row = data.frame(i,w[j,8],"Cluster")
    AS_se = rbind(AS_se, new_row)
    
    # Declare the node hosting the SE service
    t$addTag("host", attrs=c(id=w[j,8], power="5Gf"))
  
    # Declare the network interconnection of this local SE
    to_se=transfers[transfers$Destination == w[j,8],]
    from_se=transfers[transfers$Source == w[j,8],]
    
    if (nrow(to_se)>0 & nrow(from_se)>0){
      # This SE has been used for both upload(s) and download(s)
      # Then, declare two links with distinct bandwidth values
      # Discard the unrealistically low measures (< 100kB/s) while computing
      t$addTag("link", attrs=c(id=paste(w[j,8],"link_UP",sep="_"),
                               bandwidth=paste(round(mean(
                                 from_se[from_se$Bandwidth>100,10],2)),
                                               "kBps", sep=""),
                               latency="500us"))
      t$addTag("link", attrs=c(id=paste(w[j,8],"link_DOWN",sep="_"),
                               bandwidth=paste(round(mean(
                                 to_se[to_se$Bandwidth>100,10]),2),
                                               "kBps", sep=""),
                               latency="500us"))
    } else {
      # This SE has been either for upload(s) or download(s)
      # Then, declare a single full-duplex link
      # Discard the unrealistically low measures (< 100kB/s) while computing
      t$addTag("link", attrs=c(id=paste(w[j,8],"link",sep="_"),
                               bandwidth=paste(round(mean(
                                 transfers[transfers$Bandwidth>100,10]),2),
                                               "kBps", sep=""),
                               latency="500us", sharing_policy="FULLDUPLEX"))
    }
    t$addTag("host_link", attrs=c(id=w[j,8], up=paste(w[j,8],"link_UP",sep="_"),
                                  down=paste(w[j,8],"link_DOWN",sep="_")))
  }
  t$closeTag()
}

names(AS_se) <-c("AS","NameSE", "Routing")

for (i in non_local_input_ses){
  # Definition of an AS for each identified location of input file that does not
  # belong to a grid site. Such AS only comprises the SE itself.
  # The routing method for this AS is 'None'
  
  # Registe AS name and routing method for this SE
  new_row = data.frame(paste("AS",i,sep="_"),i,"None")
  names(new_row)=c("AS", "NameSE", "Routing")
  AS_se = rbind(AS_se, new_row, deparse.level = 0)
  
  # Create the AS and declare the node hosting the SE service 
  t$addTag("AS", attrs=c(id=paste("AS",i,sep="_"), routing="None"), close=FALSE)
  t$addTag("host", attrs=c(id=i, power="5Gf"))
  t$closeTag()
}

#### Declare links between ASes
t$addTag("link", attrs=c(id="service_link", bandwidth="10Gbps", latency="500ms"))

for (i in 1:nrow(input_SE_to_site_bw)){
  t$addTag("link", attrs= c(id=paste(input_SE_to_site_bw[i,1], 
                                     input_SE_to_site_bw[i,2], sep="-"),
                            bandwidth=paste(input_SE_to_site_bw[i,3],"kBps", 
                                            sep=""),
                            latency="500us"))
} 

for (i in 1:nrow(localSE_to_merge)){
  info <- AS_se[as.character(AS_se$NameSE) == localSE_to_merge[i,1],]
  
  t$addTag("link", attrs= c(id=paste(info$AS, 
                                     localSE_to_merge[i,2], sep="-"),
                            bandwidth=paste(localSE_to_merge[i,3],"kBps", 
                                            sep=""),
                            latency="500us"))
}

#### Declare the routing between ASes
# from the 'Services' AS to all the other ASes (grid sites and input locations)
for (i in sites){
  t$addTag("ASroute", attrs=c(src="Services", dst=paste("AS", i, sep="_"),
                               gw_src="Services_router", 
                               gw_dst=paste("AS",i,"router", sep="_")), 
           close=FALSE)
  t$addTag("link_ctn", attrs=c(id="service_link"))
  t$closeTag()  
}

for (i in non_local_input_ses){
  t$addTag("ASroute", attrs=c(src="Services", dst=paste("AS", i, sep="_"),
                               gw_src="Services_router", gw_dst=i), 
           close=FALSE)
  t$addTag("link_ctn", attrs=c(id="service_link"))
  t$closeTag()  
}

# From input locations to grid sites
for (i in 1:nrow(input_SE_to_site_bw)){
  t$addTag("ASroute", attrs=c(src=paste("AS", input_SE_to_site_bw[i,1], 
                                        sep="_"), 
                               dst=paste("AS", input_SE_to_site_bw[i,2], 
                                         sep="_"),
                               gw_src=as.character(input_SE_to_site_bw[i,1]), 
                               gw_dst=paste("AS",input_SE_to_site_bw[i,2],
                                            "router", sep="_")), 
           close=FALSE)
  t$addTag("link_ctn", attrs=c(id=paste(input_SE_to_site_bw[i,1], 
                                        input_SE_to_site_bw[i,2], sep="-")))
  t$closeTag()   
}

# From local SEs to merge location
for (i in 1:nrow(localSE_to_merge)){
    info <- AS_se[as.character(AS_se$NameSE) == localSE_to_merge[i,1],]

    if (info$Routing == "Cluster"){
      t$addTag("ASroute", attrs=c(src=paste("AS",info$AS, sep="_"),
                                   dst=paste("AS",localSE_to_merge[i,2], 
                                             sep="_"),
                                   gw_src=paste("AS",info$AS,"router", 
                                                sep="_"), 
                                   gw_dst=paste("AS",localSE_to_merge[i,2],
                                                "router", sep="_")), 
             close=FALSE)
    } else {
      t$addTag("ASroute", attrs=c(src=as.character(info$AS), 
                                  dst=paste("AS",localSE_to_merge[i,2], 
                                            sep="_"),
                                  gw_src=info$NameSE, 
                                  gw_dst=paste("AS",ext_downloads_merge[i,2],
                                               "router", sep="_")), 
      close=FALSE)
    }
    t$addTag("link_ctn", attrs=c(id=paste(info$AS, localSE_to_merge[i,2], 
                                          sep="-")))
    t$closeTag()   
}

# Close the initial <AS> tag
t$closeTag()

# Save the XML tree to disk
cat(saveXML(t), file=paste(output_dir,"/AS_platform_",workflow_name,".xml", sep=""))
