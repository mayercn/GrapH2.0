package algorithms.clique;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import algorithms.GatheredData;
import algorithms.VertexData;
import util.Util;

public class GatheredDataClique implements GatheredData {

	private Set<Clique> neighborCliques = new HashSet<Clique>();
	private Set<Integer> neighbors = new HashSet<Integer>();
	private int max = 0;
	
	public Set<Integer> getNeighbors() {
		return neighbors;
	}
	
	public void addNeighbor(Integer neighbor) {
		neighbors.add(neighbor);
	}
	
	public void addAllNeighbors(Collection<Integer> l) {
		for (Integer neighbor : l) {
			neighbors.add(neighbor);
		}
	}
	
	public Set<Clique> getNeighborCliques() {
		return neighborCliques;
	}
	
	/**
	 * Adds all cliques to the gathered cliques,
	 * if they are not already present.
	 * @param l
	 */
	public void addAll(Collection<Clique> l) {
//		neighborCliques.addAll(l);
		for (Clique c : l) {
			add(c);
		}
	}
	
	public void add(Clique c) {
		if (c.size()>max) {
			neighborCliques = new HashSet<Clique>();
			neighborCliques.add(c);
			max = c.size();
		} else if (c.size()==max) {
			neighborCliques.add(c);
		}
	}
	
	@Override
	public int byteSize() {
		int b = 0;
		for (Clique c : neighborCliques) {
			b += c.size() * 4;
		}
		b += neighbors.size() * 4;
		b += 4;
		return b;
	}
}
