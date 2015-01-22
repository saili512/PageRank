package com.ufl.pagerank;

import org.apache.commons.io.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import java.util.Map;

public class PageRank {

	static TreeMap<String, String> map = new TreeMap<String, String>();

	public static void main(String[] args) throws Exception {

		String inputPath = args[0];
		String outputPath = args[1];

		iterativeMapReduce(inputPath, outputPath);
	}

	public static void iterativeMapReduce(String inputPath, String outputDir)
			throws Exception {

		int iteration = 1;
		double desiredConvergence = 0.005; // specify the desired convergence
		Path jobOutputPath = null;
		Configuration conf = new Configuration();
		Path out = new Path(outputDir);
		if (out.getFileSystem(conf).exists(out))
			out.getFileSystem(conf).delete(out, true);
		out.getFileSystem(conf).mkdirs(out);

		Path in = new Path(out, "initialInput.txt");
		Path infile = new Path(inputPath);

		FileSystem fs = infile.getFileSystem(conf);
		int noOfNodes = getNoOfNodes(infile);
		double initialPageRank = 1.0 / (double) noOfNodes; // initialize page
															// rank to 1 by no.
															// of nodes for all
															// nodes

		OutputStream os = fs.create(in);
		LineIterator iter = IOUtils.lineIterator(fs.open(infile), "UTF8"); // open
																			// the
																			// input
																			// file
		// iterate through the file and write it to another file with pageranks
		// for each node next to nodeid
		while (iter.hasNext()) {
			String line = iter.nextLine();
			String[] parts = line.toString().split(" ");

			Node node = new Node().setPageRank(initialPageRank)
					.setAdjacencyList(
							Arrays.copyOfRange(parts, 1, parts.length));
			IOUtils.write(parts[0] + '\t' + node.printNode() + '\n', os);
		}
		os.close();
		// iterative map reduce until desired convergence is achieved
		while (true) {

			jobOutputPath = new Path(out, String.valueOf(iteration)); // set the
																		// output
																		// path
																		// of
																		// the
																		// reduceer
			conf.setInt(PageRankReducer.NO_OF_NODES, noOfNodes); // set the no
																	// of nodes
																	// in the
																	// graph in
																	// configuration
																	// to be
																	// accessed
																	// from
																	// reducer

			Job job = Job.getInstance(conf);
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, in);
			FileOutputFormat.setOutputPath(job, jobOutputPath);

			if (!job.waitForCompletion(true)) {
				throw new Exception("Job failed");
			}

			// get the sum of convergence value from the reducers
			long sumOfConvergence = job.getCounters()
					.findCounter(PageRankReducer.Counter.CDELTA).getValue();
			// calculate the convergence based on the sum of convergence,the
			// convergence scaling factor and the no of nodes
			double convergence = ((double) sumOfConvergence / PageRankReducer.CSF)
					/ (double) noOfNodes;

			// if convergence value calculated above is less than the desired
			// convergence then break from the iterations
			if (convergence < desiredConvergence) {
				break;
			}
			// else set the input path for the next iteration of map as the
			// output path of the reducer.
			in = jobOutputPath;
			iteration++;
		}

		sortAndListTopTenPages(jobOutputPath, out);

	}

	/**
	 * This function reads the final part files created by the reducer and puts
	 * the values of nodes and their page ranks in a tree map which is
	 * inherently stores it in sorted order. It then writes the top ten nodes
	 * and their page ranks in an output file along with the graph data i.e no.
	 * of nodes, no.of edges etc.
	 * 
	 * @param outputPath
	 * @param out
	 * @throws IOException
	 */
	public static void sortAndListTopTenPages(Path outputPath, Path out)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = outputPath.getFileSystem(conf);
		ContentSummary cs = fs.getContentSummary(outputPath);
		long fileCount = cs.getFileCount();
		// read each part file from the final reduce step
		for (int i = 0; i < fileCount; i++) {
			Path file = new Path(outputPath, "part-r-0000"
					+ Integer.toString(i));
			LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
			while (iter.hasNext()) {
				String line = iter.nextLine();
				String[] parts = line.toString().split(" ");
				if (parts.length == 1) {
					map.put(Integer.toString(0), parts[0]); // if the node is a
															// dangling node
															// give it a page
															// rank of zero
				} else {
					map.put(parts[1], parts[0]); // put the value of page rank
													// and node id in the map.
				}
			}
		}

		Path output = new Path(out, "sortedPageRanks.txt");
		OutputStream os = fs.create(output);
		// write the top ten nodes with highest page ranks to a new file
		for (int i = 10; i > 0; i--) {
			Map.Entry<String, String> entry = map.pollLastEntry();
			IOUtils.write(entry.getValue() + ',' + entry.getKey() + '\n', os);
		}
		os.close();

	}

	/**
	 * This method return the no. of Nodes in the graph based on the number of lines.
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int getNoOfNodes(Path file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);

		return IOUtils.readLines(fs.open(file), "UTF8").size();

	}
}
