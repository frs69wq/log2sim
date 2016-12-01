log2sim
============

Extracting information from execution logs of GATE workflow to generate simulation inputs

Requirements:

+ H2 database engine (v1.3.176) 
  + Available at http://www.h2database.com/html/download.html
  + Only the JAR file is needed
+ The VIPSimulator 
  + Available at https://github.com/frs69wq/VIPSimulator
+ Recent version of SimGrid
  + git clone git://scm.gforge.inria.fr/simgrid/simgrid.git 

Environment variables setting:
  + To parse original VIP/GATE execution logs
     + export LOG2SIM_LOGS=/path/to/original/logs (e.g., /home/user/log_dir)
     + export H2DRIVER=/path/to/h2-1.3.176.jar    (e.g., /home/user/h2-1.3.176.jar)
  + To regenerate some files
     + export LOG2SIM_HOME=/path/to/log2sim       (e.g., /home/user/log2sim/)
  + To replay the execution in simulation
     + export VIPSIM=/path/to/VIP/Java/Simulator  (e.g., /home/user/VIPSimulator)
     + export SIMGRID_PATH=/path/to/simgrid.jar   (e.g., /usr/local/java)
  