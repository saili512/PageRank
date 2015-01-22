package com.ufl.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		context.write(key, value); // first emit(nodeid,N)

		Node node = Node.afterMapReduce(value.toString());
		//for all nodes in the adjacency list 
		if (node.getAdjacencyList() != null
				&& node.getAdjacencyList().length > 0) {
			double outPageRank = node.getPageRank()
					/ (double) node.getAdjacencyList().length; //calculate the new page rank by dividing the page rank of the node by the number of
																// adjacent outgoing nodes
			for (int i = 0; i < node.getAdjacencyList().length; i++) {

				String adjacentNOde = node.getAdjacencyList()[i];

				outKey.set(adjacentNOde);

				Node adjacentNode = new Node().setPageRank(outPageRank);

				outValue.set(adjacentNode.printNode());
				context.write(outKey, outValue); //emit(nodeid, pagerank) 
			}
		}
	}
}
