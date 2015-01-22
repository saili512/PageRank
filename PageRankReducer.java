package com.ufl.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	public static final double CSF = 1000.0; // convergence scaling factor
	public static final double DF = 0.85; // damp factor
	public static String NO_OF_NODES = "pagerank.numnodes";
	private int totalNoOfNodes;

	public static enum Counter {
		CDELTA
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		totalNoOfNodes = context.getConfiguration().getInt(
				NO_OF_NODES, 0);
	}

	private Text outValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double sum = 0;
		double dampingFactor = ((1.0 - DF) / (double) totalNoOfNodes); //intialize the damping factor
		Node realNode = new Node();
		//for all values for a given key i.e. nodeId
		for (Text textValue : values) {

			Node node = Node.afterMapReduce(textValue.toString());
			//if the node is a real node then store it in a variable else it is a pagerank value so add it to the sum
			if (node.isNode()) {
				realNode = node;
			} else {
				sum += node.getPageRank();
			}
		}

		double newPageRank = dampingFactor + (DF * sum); //calculate the new page rank for the node taking into consideration damping factor

		double delta = realNode.getPageRank() - newPageRank;

		realNode.setPageRank(newPageRank);

		outValue.set(realNode.printNode());
		context.write(key, outValue); // write the nodeid and the node to conetxt

		int scaledDelta = Math.abs((int) (delta * CSF));

		context.getCounter(Counter.CDELTA).increment(scaledDelta); // set the scaled delta as a counter 
	}
}
