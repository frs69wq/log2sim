log2sim
============

Extracting information from execution logs of GATE workflow to generate simulation inputs

Requirements:

+ H2 database engine (v1.3.176) 
  + Available at http://www.h2database.com/html/download.html
  + Only the JAR file is needed
+ The VIPSimulator 
  + Available at https://github.com/frs69wq/VIPSimulator
+ A stable Java archive of SimGrid
  + Available at http://gforge.inria.fr/frs/download.php/file/35216/simgrid.jar

Environment variables setting:
  + To parse original VIP/GATE execution logs
     + export LOG2SIM_LOGS=/path/to/original/logs (e.g., /home/user/log_dir)
     + export H2DRIVER=/path/to/h2-1.3.176.jar    (e.g., /home/user/h2-1.3.176.jar)
  + To replay the execution in simulation
     + export VIPSIM=/path/to/VIP/Java/Simulator  (e.g., /home/user/VIPSimulator)
     + export SIMGRID_PATH=/path/to/simgrid.jar   (e.g., /usr/local/java)
  