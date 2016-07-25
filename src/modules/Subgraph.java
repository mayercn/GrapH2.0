package modules;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import util.Edge;
import util.Util;
import util.Vertex;

public class Subgraph {
	
	/*
	 * Graph representation: adjacency lists, but sets instead of lists to guarantee O(1) access for contains
	 */
	private TIntObjectHashMap<TIntHashSet> inEdges = new TIntObjectHashMap<TIntHashSet>(100000);
	private TIntObjectHashMap<TIntHashSet> outEdges = new TIntObjectHashMap<TIntHashSet>(100000);
	private TIntObjectHashMap<Vertex> vertices = new TIntObjectHashMap<Vertex>(100000);
//	private HashMap<Integer, Vertex> vertices = new HashMap<Integer, Vertex>(100000);	// TODO: change to trove
//	private HashMap<Integer, HashSet<Edge>> inEdges = new HashMap<Integer, HashSet<Edge>>(100000);
//	private HashMap<Integer, HashSet<Edge>> outEdges = new HashMap<Integer, HashSet<Edge>>(100000);
	private Partition p;
	
	private int numberOfEdges = 0;
	
	public Subgraph(String edgeFile, Partition p) throws NumberFormatException, IOException {
		this.p = p;
		initializeEdges(edgeFile);
		
	}
	
	private void initializeEdges(String edgeFile) throws NumberFormatException, IOException {

		// 1. load all edges and vertices into memory
		BufferedReader br = new BufferedReader(new FileReader(new File(edgeFile)), 1024 * 8000);
		String line;
		System.out.println("Start reading edges from file " + edgeFile);
		while((line = br.readLine())!=null) {
			String s[] = line.split("\\s+");
			Integer u = Integer.valueOf(s[0]);
			Integer v = Integer.valueOf(s[1]);
			if (u==v) {
				// Skip self-edges
				continue;
			}
			if (!vertices.containsKey(u)) {
				newVertex(u);
			}
			if (!vertices.containsKey(v)) {
				newVertex(v);
			}
			Edge e = new Edge(u, v);
			newEdge(e);
		}
		br.close();
		System.out.println("Edges read!");
	}

//	private HashMap<Integer, HashSet<Edge>> getInEdges() {
//		return inEdges;
//	}
//	
//	private HashMap<Integer, HashSet<Edge>> getOutEdges() {
//		return outEdges;
//	}

	public TIntObjectHashMap<Vertex> getVertices() {
		return vertices;
	}

	public boolean contains(int vertex_id) {
		return vertices.containsKey(vertex_id);
	}

	public int getLoad() {
		int total = 0;
		for ( TIntObjectIterator<Vertex> it = vertices.iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			total += v.trafficPrediction();
		}
		return total;
	}
	
	/**
	 * Returns in and out neighbors of vertex v
	 * @param v
	 * @return
	 */
	public List<Vertex> getNeighbors(Vertex v) {
		List<Vertex> neighbors = new ArrayList<Vertex>();
		TIntIterator iter = getInEdges(v).iterator();
		while (iter.hasNext()) {
			int neighborID = iter.next();
			Vertex neighbor = vertices.get(neighborID);
			if (!neighbors.contains(neighbor)) {
				neighbors.add(neighbor);
			}
		}
		iter = getOutEdges(v).iterator();
		while (iter.hasNext()) {
			int neighborID = iter.next();
			Vertex neighbor = vertices.get(neighborID);
			if (!neighbors.contains(neighbor)) {
				neighbors.add(neighbor);
			}
		}
		return neighbors;
	}

	/**
	 * Returns in neighbors of vertex v
	 * @param v
	 * @return
	 */
	public List<Vertex> getInNeighbors(Vertex v) {
		List<Vertex> neighbors = new ArrayList<Vertex>();
		TIntIterator iter = getInEdges(v).iterator();
		while (iter.hasNext()) {
			int neighborID = iter.next();
			Vertex neighbor = vertices.get(neighborID);
			neighbors.add(neighbor);
		}
		return neighbors;
	}

	/**
	 * Returns out neighbors of vertex v
	 * @param v
	 * @return
	 */
	public List<Vertex> getOutNeighbors(Vertex v) {
		List<Vertex> neighbors = new ArrayList<Vertex>();
		TIntIterator iter = getOutEdges(v).iterator();
		while (iter.hasNext()) {
			int neighborID = iter.next();
			Vertex neighbor = vertices.get(neighborID);
			neighbors.add(neighbor);
		}
		return neighbors;
	}

	/**
	 * Returns the list of out edges from vertex v or
	 * an empty list, if the vertex has none or does not exist.
	 * @param v
	 * @return
	 */
	public TIntHashSet getOutEdges(Vertex v) {
		TIntHashSet outNeighbors = outEdges.get(v.id());
		if (outNeighbors != null) {
			return outNeighbors;
		} else {
			return new TIntHashSet();
		}
	}

	/**
	 * Returns the list of in edges from vertex v or
	 * an empty list, if the vertex has none or does not exist.
	 * @param v
	 * @return
	 */
	public TIntHashSet getInEdges(Vertex v) {
		TIntHashSet outNeighbors = inEdges.get(v.id());
		if (outNeighbors != null) {
			return outNeighbors;
		} else {
			return new TIntHashSet();
		}
	}

	/**
	 * Removes the vertex with given id
	 * @param v_id
	 */
	public void removeVertex(Integer v_id) {
		// TODO: remove debug
		if (!vertices.containsKey(v_id) || (!inEdges.containsKey(v_id) && !outEdges.containsKey(v_id))) {
			System.out.println("Vertex not present: Error!! " + v_id);
			System.out.println(vertices.get(v_id));
			System.out.println(inEdges.get(v_id));
			System.out.println(inEdges.containsKey(v_id));
			System.out.println(outEdges.get(v_id));
			System.exit(-1);
		}
		vertices.remove(v_id);
		inEdges.remove(v_id);
		outEdges.remove(v_id);
	}

	/**
	 * Removes the edge e from the Partition
	 * @param e
	 */
	public void removeEdge(Edge e) {	// TODO: input id1, id2 instead of wrapper edge
		if (inEdges.containsKey(e.v)) {
			inEdges.get(e.v).remove(e.u);
			outEdges.get(e.u).remove(e.v);
			numberOfEdges--;
		}
	}

	/**
	 * Adds the specified edge to the partition.
	 * @param u
	 * @param v
	 */
	public void newEdge(Edge e) {
		if (!inEdges.containsKey(e.v)) {
			inEdges.put(e.v, new TIntHashSet());
		}
		if (!outEdges.containsKey(e.u)) {
			outEdges.put(e.u, new TIntHashSet());
		}
		if (!inEdges.get(e.v).contains(e.u)) {
			inEdges.get(e.v).add(e.u);
			outEdges.get(e.u).add(e.v); 
			numberOfEdges++;
		}
	}
	

	/**
	 * Initializes the vertex v_id with the initial data
	 * @param v_id
	 */
	private void newVertex(int v_id) {
		List<Integer> l = new ArrayList<Integer>();
		l.add(p.id());
		Vertex v = new Vertex(v_id, p.id(), l, null, 1, 1, false);
//		VertexData v_data = p.toExecute.get(0).getVertexData(v_id, null, vertexFile);
//		v.setData(v_data);
//		v.flushData();
		vertices.put(v_id, v);
		inEdges.put(v_id, new TIntHashSet());
		outEdges.put(v_id, new TIntHashSet());
	}

	/**
	 * Returns the number of local edges
	 * @return
	 */
	public int getNumberOfEdges() {
		return numberOfEdges;
	}
	
	public String toString() {
		return vertices + "\n" + inEdges;
	}

	
	public static void main(String[] args) {
//		HashSet<Integer> l = new HashSet<Integer>();
//		long time = System.nanoTime();
//		for (int i=0; i<1000000; i++) {
//			l.add(i);
//		}
////		System.out.println(l);
//		System.out.println("Time needed: " + (System.nanoTime()-time));
//		
//		TIntHashSet l2 = new TIntHashSet();
//		time = System.nanoTime();
//		for (int i=0; i<1000000; i++) {
//			l2.add(i);
//		}
////		System.out.println(l2);
//		System.out.println("Trove: Time needed: " + (System.nanoTime()-time));
//		
//		time = System.nanoTime();
//		for (Integer i : l) {
//			i = i+1;
//		}
//		System.out.println("Time needed: " + (System.nanoTime()-time));
//		
//		time = System.nanoTime();
//		TIntIterator iter2 = l2.iterator();
//		while (iter2.hasNext()) {
//			int i = iter2.next();
//			i = i+1;
//		}
//		System.out.println("Trove: Time needed: " + (System.nanoTime()-time));
		
		TIntObjectHashMap<Object> map = new TIntObjectHashMap<Object>();
		for (int i=0; i<1000000; i++) {
			map.put(i, new Object());
		}
		System.out.println("Size: " + map.size());
		long time = System.nanoTime();
		int count = 0;
		TIntObjectIterator<Object> iter = map.iterator();
		while (iter.hasNext()) {
			iter.advance();
			Object o = iter.value();
			Object v = o;
			if (v==o) {
				count++;
			}
		}
		System.out.println("Time: " + (System.nanoTime()-time));
		System.out.println("Iterated: " + count);
		
		HashMap<Integer, Object> map2 = new HashMap<Integer, Object>();
		for (int i=0; i<1000000; i++) {
			map2.put(i, new Object());
		}
		
		time = System.nanoTime();
		int count2 = 0;
		for ( Object o : map2.values()) {
			Object v = o;
			if (v==o) {
				count2++;
			}
		}
		System.out.println("Time: " + (System.nanoTime()-time));
		System.out.println("Iterated: " + count2);
		
	}
	
}
