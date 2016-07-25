package algorithms.clique;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import algorithms.GAS_interface;
import algorithms.GatheredData;
import algorithms.VertexData;

import util.Edge;
import util.Vertex;
import modules.Partition;


public class GAS_Clique implements GAS_interface {
	
	private Partition p;
	private int GOALVERTEX = -1;	// the id of the vertex that should be part of the clique.
	
	public GAS_Clique(Partition p, int v_id) {
		this.p = p;
		this.GOALVERTEX = v_id;
	}

	/**
	 * Specifies whether to gather on in-edges, out-edges or all edges
	 * @return
	 */
	public int gather_edges() {
		return GAS_interface.ALL_EDGES;
	}
	
	/**
	 * Gathers the vertex data from the neighbor
	 * @param neighbor
	 * @return
	 */
	@Override
	public GatheredDataClique gather(Vertex myself, Vertex neighbor, Edge e) {
		GatheredDataClique gathered = new GatheredDataClique();
		VertexDataClique neighborData = (VertexDataClique) neighbor.data();
		gathered.addNeighbor(neighbor.id());
//		gathered.doubleVal = (double)neighborData.rank / (double)neighborData.outDegree;
		gathered.addAll(neighborData.getAllCliques_NoCopy());
		if (gathered.getNeighborCliques().size()==0) {
			Set<Integer> l = new HashSet<Integer>();
			l.add(neighbor.id());
			gathered.add(new Clique(l));
		}
		return gathered;
	}
	
	/**
	 * Sums two gathered objects together.
	 * @param a
	 * @param b
	 * @return
	 */
	@Override
	public GatheredDataClique sum(GatheredData a, GatheredData b) {
		GatheredDataClique gathered = new GatheredDataClique();
		gathered.addAll(((GatheredDataClique)a).getNeighborCliques());
		gathered.addAll(((GatheredDataClique)b).getNeighborCliques());
		gathered.addAllNeighbors(((GatheredDataClique)a).getNeighbors());
		gathered.addAllNeighbors(((GatheredDataClique)b).getNeighbors());
		return gathered;
	}

	@Override
	public void applyScatter(Vertex v, GatheredData sum) {
		VertexDataClique newData = (VertexDataClique) v.data();
		GatheredDataClique sumG = (GatheredDataClique) sum;
//		System.out.println(v.id() + " gathered cliques: " + sumG.getNeighborCliques());
//		System.out.println(v.id() + " gathered cliques size: " + sumG.getNeighborCliques().size());
		Set<Clique> newCliques = new HashSet<Clique>();
		int largestClique = newData.largestClique();
		for (Clique c : sumG.getNeighborCliques()) {
			if (c.size()>=largestClique-1) {
				if ( c.containsVertex(v.id()) || c.match(sumG.getNeighbors())) {
					Set<Integer> l = c.getCliqueCopy();
					l.add(v.id());
					Clique c1 = new Clique(l);
					if (c1.size()>largestClique && c1.containsVertex(GOALVERTEX)) {
						largestClique = c1.size();
						newCliques = new HashSet<Clique>();
						newCliques.add(c1);
					} else if (c1.size()==largestClique && c1.containsVertex(GOALVERTEX)){
						newCliques.add(c1);
					}
				}
			}
		}
//		System.out.println(newData.getAllCliques());
//		System.out.println("New cliques: " + newCliques);
		Iterator<Clique> iter = newCliques.iterator();
		while (iter.hasNext()) {
			if (newData.containsClique((Clique)iter.next())) {
				iter.remove();
			}
		}
		if (newCliques.size()>0) {
			newData.addAll(newCliques);
			v.setData(newData);
			/*
			 * Make data visible directly. This is no problem, because
			 * neighboring vertices can gather the larger cliques and learn
			 * earlier about these.
			 */
//			v.flushData();
//			System.out.println("Vertex data: " + v.data());
//			System.out.println("New Vertex data: " + v.newData());
			v.signalOutNeighbors(true);
			v.signalInNeighbors(true);
//			System.out.println(v.id() + " new cliques found: " + " " + newCliques);
			System.out.println(v.id() + " largest cliques: " + largestClique);
		}
//		System.out.println("---------------");
	}

	@Override
	public VertexData getVertexData(Integer v_id, VertexData oldVertexData, String vertexFile) {
		VertexDataClique data = new VertexDataClique();
		Set<Integer> s = new HashSet<Integer>();
//		s.add(v_id);
		Clique c = new Clique(s);
		data.add(c);
		return data;
	}

	@Override
	public VertexData getEmptyVertexData() {
		return new VertexDataClique();
	}

	@Override
	public GatheredData getEmptyGatheredData() {
		return new GatheredDataClique();
	}
}
