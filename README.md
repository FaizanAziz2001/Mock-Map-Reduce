# Mock-Map-Reduce
Mock Map Reduce implemented in C++ using gRPC and HDFS. The input is taken and stored on hdfs which is then used to create chunks and use slave nodes to generated key value pairs which are then compiled by the master. Protobuf is used for communication between slave and master gRPC communication. Multi threading is used for each slave to perform its own task for better concurrency.
Number of maximum slaves is set 3 and addresses are hard coded but can be changed as required.

**Important notice:**
HDFS installation is user dependent hence the directories given in the code is according to my installation directory and may require changes for your specific computer

**Things required to run:**
- Protobuf  and gRPC
- HDFS (file is given for instructions)
- Ubuntu or any linux distro

**To Run:**
- make cmake/build folder
- mkdir -p cmake/build
- cd cmake/build
- cmake ../..
- make -j8
- run the server and master exe
- For slave the command line agruments is ./slave portnumber(must be 5000,5001 or 5002)
- For master ./master

