#!/usr/bin/Rscript
##############################################################################
# Copyright (c) Centre de Calcul de l'IN2P3 du CNRS, CREATIS                 #
# Contributor(s) : Frédéric SUTER (2015-2016)                                #
#                                                                            #
# This program is free software; you can redistribute it and/or modify it    #
# under the terms of the license (GNU LGPL) which comes with this code.      #
##############################################################################

#### Required R packages
library(XML)
library(reshape2)
#### Parsing command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) < 1) {
  stop("Usage: deployment_generator.R <workflow_name> [initial | standalone]", call.=FALSE)
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

catalog_name=paste(paste("LfcCatalog",workflow_name, sep="_"), "csv", sep=".")
defSE="ccsrm02.in2p3.fr" 

# load all needed files 
catalog <- read.csv(paste(output_dir,catalog_name, sep="/"), header=FALSE, sep=',',as.is=TRUE)
names(catalog)<-c("file", "size", "locations")
raw_transfers <- read.csv(paste(wd,'file_transfer.csv', sep="/"), header = TRUE, sep=',',as.is=TRUE)
db_dump <- read.csv(paste(wd,'db_dump.csv', sep="/"), header=TRUE, sep=" ", as.is=TRUE)

#List all SEs but the default one
storage_elements <- unique(c(raw_transfers[raw_transfers$UpDown == 1,]$Destination,
                             raw_transfers[raw_transfers$UpDown == 2,]$Source))
storage_elements = storage_elements[storage_elements != defSE]

#List SEs used to download inputs but the default one
inputSEs <- unique(unlist(strsplit(paste(catalog$locations, collapse=":"),":")))
inputSEs = inputSEs[inputSEs != defSE]

# extract Gate job arguments from database and file transfers 
start_and_compute_times = db_dump[c(1,7,10)]
uploads_gate <- raw_transfers[raw_transfers$UpDown == 1 & raw_transfers$JobType == "gate",
                              c(2,3,5,7)]
download_sources_gate <- 
  raw_transfers[raw_transfers$UpDown == 2 & raw_transfers$JobType=="gate",c(2,3)]
download_sources_gate$File<-rep(c("wrapper","distrib","input"))

sources_by_gate_job = reshape(download_sources_gate, timevar = "File", idvar="JobId", 
                         direction = "wide")

gate_arguments <- merge(merge(uploads_gate, start_and_compute_times), sources_by_gate_job)

uploads_merge <- raw_transfers[raw_transfers$UpDown == 1 & 
                                 raw_transfers$JobType == "merge" &
                                 raw_transfers$FileSize > 12,
                              c(2,3,5,7)]
download_sources_merge <- 
  head(raw_transfers[raw_transfers$UpDown == 2 & raw_transfers$JobType=="merge",c(2,3)],
       n=2)
download_sources_merge$File<-c("wrapper","input")
sources_by_merge_job = reshape(download_sources_merge, timevar = "File", idvar="JobId", 
                         direction = "wide")

merge_arguments <- merge(merge(uploads_merge, start_and_compute_times), 
                         sources_by_merge_job)

#### Generation of the XML tree
# Creation and header
t = xmlTree("platform", attrs=c(version="4"), dtd='platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd"')

# VIP Server
t$addTag("process", attrs=c(host="vip.creatis.insa-lyon.fr", fun="VIPServer"))

# Storage elements
for(i in storage_elements){
  if (i %in% inputSEs){
    t$addTag("process",  attrs=c(host=i, fun="SE"), close=FALSE)
    t$addTag("argument", attrs=c(value=paste("simgrid_files", catalog_name, sep="/")))
    t$closeTag()
  } else {
    t$addTag("process",  attrs=c(host=i, fun="SE"))
  }
}

# Default SE
t$addTag("process", attrs=c(host=defSE, fun="DefaultSE"), close=FALSE)
t$addTag("argument", attrs=c(value=paste("simgrid_files", catalog_name, sep="/")))
t$closeTag()

# Default LFC
t$addTag("process", attrs=c(host="lfc-biomed.in2p3.fr", fun="DefaultLFC"), close=FALSE)
t$addTag("argument", attrs=c(value=paste("simgrid_files", catalog_name, sep="/")))
t$closeTag()

# Gate and Merge Jobs
for (i in 1:nrow(gate_arguments)){
  t$addTag("process", attrs=c(host=gate_arguments[i,2], fun="Gate", 
                              start_time=gate_arguments[i,5]), close=FALSE)
  t$addTag("argument", attrs=c(value=gate_arguments[i,1]))
  t$addTag("argument", attrs=c(value=gate_arguments[i,6]))
  t$addTag("argument", attrs=c(value=gate_arguments[i,3]))
  t$addTag("argument", attrs=c(value=gate_arguments[i,7]))
  t$addTag("argument", attrs=c(value=gate_arguments[i,8]))
  t$addTag("argument", attrs=c(value=gate_arguments[i,9]))
  
  t$closeTag()
}

t$addTag("process", attrs=c(host=merge_arguments$Source, fun="Merge"), close=FALSE)
t$addTag("argument", attrs=c(value=merge_arguments$JobId))
t$addTag("argument", attrs=c(value=merge_arguments$ComputeDuration))
t$addTag("argument", attrs=c(value=merge_arguments$FileSize))
t$addTag("argument", attrs=c(value=merge_arguments$Source.wrapper))
t$addTag("argument", attrs=c(value=merge_arguments$Source.input))
t$closeTag()

# Save the XML tree to disk
f=paste(output_dir,"/deployment_",workflow_name,"_2.xml", sep="")
cat(saveXML(t), file=f)
# Replace fun by function
y=readLines(f, warn = FALSE)
cat(gsub("fun","function", y), file=f, sep="\n")
