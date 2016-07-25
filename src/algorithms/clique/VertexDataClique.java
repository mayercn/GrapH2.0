package algorithms.clique;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import algorithms.VertexData;
import util.Util;

public class VertexDataClique implements VertexData {
	
	// Clique
	private Set<Clique> largestCliquesSeen;
	private int max; // returns the size of the maximal clique
	
	public VertexDataClique(Set<Clique> largestCliquesSeen2, int max2) {
		this.largestCliquesSeen = largestCliquesSeen2;
		this.max = max2;
	}
	
	public VertexDataClique() {
		this.largestCliquesSeen = new HashSet<Clique>();
		this.max = 1;
	}

	private Set<Clique> getAllCliques() {
		return copySet(largestCliquesSeen);
	}
	
	public Set<Clique> getAllCliques_NoCopy() {
		return largestCliquesSeen;
	}
	
	public int largestClique() {
		return max;
	}
	
	@Override
	public int byteSize() {
		int b = 0;
		for (Clique c : largestCliquesSeen) {
			b += c.size() * 4;
		}
		b += 4;
		return b;
	}
	
	private static Set<Clique> copySet(Set<Clique> cliques) {
		Set<Clique> s = new HashSet<Clique>();
		for (Clique c : cliques) {
			s.add(c.copy());
		}
		return s;
	}
	

	/**
	 * Adds all cliques to the vertex' largest cliques
	 * @param newCliques
	 */
	public void addAll(Set<Clique> newCliques) {
		for (Clique c : newCliques) {
			add(c);
		}
	}

	/**
	 * Adds clique to the vertex'largest cliques
	 * @param c
	 */
	public void add(Clique c) {
		if (c.size()==max) {
//			if (!this.largestCliquesSeen.contains(c)) {
				this.largestCliquesSeen.add(c);
//			}
		} else if (c.size()>max) {
			this.largestCliquesSeen = new HashSet<Clique>();
			this.largestCliquesSeen.add(c);
			this.max = c.size();
		}
	}

	public boolean containsClique(Clique c) {
		return largestCliquesSeen.contains(c);
	}
}
