#!/usr/bin/Rscript

library(methods)
library(XML)
library(plyr)
### Parsing command line arguments
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

workers <- read.csv(paste(wd,'worker_nodes.csv', sep="/"), header=TRUE, sep=',', as.is=TRUE)
transfers <- read.csv(paste(wd,'file_transfer.csv', sep="/"), header = TRUE, sep=',')
transfers=transfers[transfers$FileSize >12,]
transfers$Bandwidth <- transfers$FileSize/(transfers$Time-990)
transfers = transfers[transfers$Bandwidth>100,]

t = xmlTree("platform", attrs=c(version="3"), dtd='platform "http://simgrid.gforge.inria.fr/simgrid.dtd"')
t$addTag("AS", attrs=c(id=workflow_name, routing="Full"), close=FALSE)

t$addTag("AS", attrs=c(id="Services", routing="Cluster"), close=FALSE)
t$addTag("router", attrs=c(id="Services_router"))
t$addTag("backbone",attrs=c(id="Services_backbone", bandwidth="100GBps", latency="1500us"))

t$addTag("host", attrs=c(id="vip.creatis.insa-lyon.fr", power="5Gf",core="4"))
t$addTag("link", attrs=c(id="vip.creatis.insa-lyon.fr_link", bandwidth="10Gbps", latency="500us", sharing_policy="FULLDUPLEX"))
t$addTag("host_link", attrs=c(id="vip.creatis.insa-lyon.fr", up="vip.creatis.insa-lyon.fr_link_UP",
                              down="vip.creatis.insa-lyon.fr_link_DOWN"))
  
t$addTag("host", attrs=c(id="lfc-biomed.in2p3.fr", power="5Gf", core="4"))
t$addTag("link", attrs=c(id="lfc-biomed.in2p3.fr_link", bandwidth="10Gbps", latency="500us",
                         sharing_policy="FULLDUPLEX"))
t$addTag("host_link", attrs=c(id="lfc-biomed.in2p3.fr", up="lfc-biomed.in2p3.fr_link_UP",
                              down="lfc-biomed.in2p3.fr_link_DOWN"))

# t$addTag("host", attrs=c(id="ccsrm02.in2p3.fr", power="5Gf"))
# t$addTag("link", attrs=c(id="ccsrm02.in2p3.fr_link",
#                          bandwidth="10368.1kBps",
#                          latency="500us",
#                          sharing_policy="FULLDUPLEX"))
# t$addTag("host_link", id="ccsrm02.in2p3.fr", up="ccsrm02.in2p3.fr_link_UP", 
#          down="ccsrm02.in2p3.fr_link_DOWN"))
  
t$closeTag()

t$addTag("link", attrs=c(id="service_link", bandwidth="10Gbps",
                         latency="500ms"))

sites <- unique(factor(workers$SiteName))
for (i in sites){
  t$addTag("AS", attrs=c(id=i, routing="Cluster"), close=FALSE)
  t$addTag("router", attrs=c(id=paste(i,"router", sep="_")))
  t$addTag("backbone",attrs=c(id=paste(i,"backbone", sep="_"), 
                              bandwidth="100GBps", latency="1500us"))
  
  w = workers[workers$SiteName == i,]
  for (j in 1:nrow(w)){
    #cat(workers[j,2])
    t$addTag("host", attrs=c(id=w[j,2], power=w[j,4], core=w[j,3]), close=FALSE)
    t$addTag("prop", attrs=c(id="closeSE", value=w[j,8]))
    t$closeTag()
    t$addTag("link", attrs=c(id=paste(w[j,2],"link",sep="_"), bandwidth=w[j,5],
                             latency="500us", sharing_policy="FULLDUPLEX"))
    t$addTag("host_link", attrs=c(id=w[j,2],
                                  up=paste(w[j,2],"link_UP",sep="_"), 
                                  down=paste(w[j,2],"link_DOWN",sep="_")))  
  }
  
  if (length(unique(as.factor(w$closeSE)))>1){
    stop("Worker nodes of a same site declare different local SEs")
  } else{
    t$addTag("host", attrs=c(id=w[j,8], power="5Gf"))
  
    to_se=transfers[transfers$Destination == w[j,8],]
    from_se=transfers[transfers$Source == w[j,8],]
    
    if (nrow(to_se)>0 & nrow(from_se)>0){
      t$addTag("link", attrs=c(id=paste(w[j,8],"link_UP",sep="_"),
                               bandwidth=paste(round(mean(from_se$Bandwidth),2),
                                               "kBps", sep=""),
                               latency="500us"))
      t$addTag("link", attrs=c(id=paste(w[j,8],"link_DOWN",sep="_"),
                               bandwidth=paste(round(mean(to_se$Bandwidth),2),
                                               "kBps", sep=""),
                               latency="500us"))
    } else {
      t$addTag("link", attrs=c(id=paste(w[j,8],"link",sep="_"),
                               bandwidth=paste(round(mean(transfers$Bandwidth),2),
                                               "kBps", sep=""),
                               latency="500us", sharing_policy="FULLDUPLEX"))
    }
    t$addTag("host_link", attrs=c(id=w[j,8], up=paste(w[j,8],"link_UP",sep="_"),
                                  down=paste(w[j,8],"link_DOWN",sep="_")))
  }
  t$closeTag()
}

local_ses <- unique(as.factor(workers$CloseSE))
if (length(unique(transfers[!transfers$Destination %in% local_ses &
                              transfers$UpDown==1,]$Destination))>0){
  stop("Some SE are used for upload without being local.")
}

non_local_transfers <- transfers[!transfers$Source %in% local_ses &
                                   !transfers$Destination %in% local_ses,]

other_ses <- unique(non_local_transfers$Source)

for (i in other_ses){
  t$addTag("AS", attrs=c(id=paste("AS",i,sep="_"), routing="None"), close=FALSE)
  t$addTag("host", attrs=c(id=i, power="5Gf"))
  t$closeTag()
}

for (i in sites){
  t$addTag("AS_route", attrs=c(src="Services", dst=paste("AS", i, sep="_"),
                               gw_src="Services_router", 
                               dst_gw=paste(i,"router", sep="_")), 
           close=FALSE)
  t$addTag("link_ctn", attrs=c(id="service_link"))
  t$closeTag()  
}

for (i in other_ses){
  t$addTag("AS_route", attrs=c(src="Services", dst=paste("AS", i, sep="_"),
                               gw_src="Services_router", dst_gw=i), 
           close=FALSE)
  t$addTag("link_ctn", attrs=c(id="service_link"))
  t$closeTag()  
}
df =ddply(non_local_transfers,c("Source","SiteName"),summarize, AvgBandwidth=mean(Bandwidth))
t$closeTag()
cat(saveXML(t), file="toto.xml")
