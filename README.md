# PageRank
Iterative Hadoop MapReduce program to implement PageRank using Amazon EC2

# Name of the programs
1. PageRank.jar
PageRank.java & class #This is the driver class
PageRankMapper.java & class #This is the mapper class
PageRankReducer.java & class #This is the reducer class
Node.java & class # This is node definition for graph nodes

# Program Description

PageRank - This program takes an input file which has graph data in the form of node and its adjacency lists and generates files containing nodes, their page ranks and their adjacency lists. It also generates a file which has the top ten nodes listed in descending order of their page ranks and other graph data.

PageRank.java
This is the driver class.
It is the main class in which the iterations of map and reduce phase start.
It fixes a desired convergence, initializes the initial page rank to a value = (1/no of nodes in the graph)
It then read the input file and creates an intermediary input file for the first Map task.
In the iterative loop, the map and reduce tasks are defined and their input and output paths provided.
At the end of each iteration, calculate the convergence based on sum of convergence,
No. of nodes and convergence scaling factor.
If the convergence is less than the desired convergence stop the iterations.
Else set the input path for the next map phase as the output path of the current reduce phase.
After the iterations are over, the final output part files created by the reducer are read and sorted to list the top ten nodes with the highest page rank and other graph data such as no. of nodes, no. of edges etc is written to new file.

PageRankMapper.java
This class is the mapper class. It receives a nodeid as key and node information as value
It emits the nodeid and the node information first.
Calculate pagerank for each of the adjacent nodes.
For each of the nodes in its adjacency list
	It emits a key value pair of (nodeid,pagerank)
	
PageRankReducer.java
This class is the reducer class. It receives a key- nodeid and a list of values.
For each value in the list
	If it is a node, the initialize the node with this node.
Else if it a pagerank value sum it up
Set the pagerank in the initialized node.
Finally emit a key value pair of (nodeid,node)
