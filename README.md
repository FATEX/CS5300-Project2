# CS5300-Project2 Spring 2013
---------------------------------------
**Ben Perry, Chantelle Farmer & Matthew Green**

MapReduce Project - http://edu-cornell-cs-cs5300s13-assignment4.s3.amazonaws.com/proj2.html

## Running Our Code
---------------------------------------
Build the project into a JAR file and run with the inputs project2.NodeDriver s3n://in filename s3n://out bucket for the Single Node solution and with project2.PageRankBlock s3n://in filename s3n://out bucket. You can then run this on the Amazon EMR or locally using the Hadoop framework on a single node (your machine). **CAUTION:** THIS TAKES A WHILE! 

We experienced runtimes of approximately 1min per pass for the single pass and approximately 2mins per pass for the Blocked Approach.


## Filter Parameters
---------------------------------------

Compute filter parameters for netID mcg67
	double fromNetID = 0.76;
	double rejectMin = 0.99 * fromNetID; (0.7524)
	double rejectLimit = rejectMin + 0.01; (0.7624)

Number of Nodes: **678,386** from *685230*

## Code Structure
---------------------------------------
### Single Node/Power Iteration
#### NodeDriver.java
This contains our main class for the single blocked iteration. We set runtime variables such as the number of iterations and used a enum or Hadoop counter to store the residual error, this counter is accesed from the LeReducer. The driver reads from the input file which is our preprossed text and then commissions the mapper to sort and emit the data to the reducers for the computation of the PageRanks to be done. 

#### LeMapper.java
This class calculates the PageRank factor based on the PageRank(u) and degree(u) which it then sends LeReducer. It also sends along the previous PageRank and the outgoing edgelist so <u,v> for each u.

#### LeReducer.java
This class computes the new PageRank(u) using the PageRank equation (dampening factor and PageRank factor). More importantly it calculates the average residual error based off of the new and old PageRanksm this float is used for the test of convergence. It then produces a temporary output which is then picked up by the Mapper and then the process is ran once again.

### Blocked Approach/Grouping Nodes
#### PageRankBlock.java
This contains our main class for the blocked approach. We set runtime variables which are totalNodes, totalBlocks, NumberOfIterations, precision = 10000 and finall the static enum Hadoop counter for RESIDUAL_ERROR to store the overall block level residual error. It also computes the average of the residual error before the next pass starts, we use this so as to get the error to know whether or not the mapper and reducer need to be invoked again.

#### PageRankBlockMapper.java
This class computes and looks up the blockID for any given node(u) and then uses this partition the node into a block. A function called lookupBlockID is used for this process. Whereby:
	
	blockID = (int) Math.floor(nodeID / partitionSize);

is used to check the block a node should be in. This class also sends messages to the Reducer with special flags, PR for PageRank, BE for edges for nodes in block B and BC for boundary conditions for block B.

#### PageRankBlockReducer.java
This reducer class operates much like the single node approach whereby the graph within a block is traversed for 5 times or until convergence to generate the residual error. This error is computed from the incoming PageRank and then the new computed one to find out what the residual error is. This class also has a series of HashMaps that are used to store data such as NewPageRank, Boundary Conditions, Internal Connections (BE), Data for a particular node and finally a list of vertices. Overall this class is the workhorse of this blocked approach since it parses the messages from the mapper, decides what node connects with what edge, computes the PageRank and then finally emits the error after each internal pass.
 
#### NodeData.java
Data structure used to represent a node using nodeID, edgeList, pageRank, degrees.