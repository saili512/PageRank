package com.ufl.pagerank;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Arrays;

public class Node {

	private double pageRank = 0.25;
	private String[] adjacencyList;

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacencyList() {
		return adjacencyList;
	}

	public Node setAdjacencyList(String[] adjacencyList) {
		this.adjacencyList = adjacencyList;
		return this;
	}

	public boolean isNode() {
		return adjacencyList != null;
	}

	public String printNode() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);

		if (getAdjacencyList() != null) {
			sb.append('\t').append(StringUtils.join(getAdjacencyList(), '\t'));
		}
		return sb.toString();
	}

	/**
	 * This function updates the adjacency list of a node before the start of every map and reduce 
	 * task of next iteration using the values of previous iteration
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static Node afterMapReduce(String value) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(value, '\t');
		if (parts.length < 1) {
			throw new IOException();
		}
		Node node = new Node().setPageRank(Double.valueOf(parts[0]));
		if (parts.length > 1) {
			node.setAdjacencyList(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}
}
