package algorithms.clique;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Clique {

	public Clique(Set<Integer> clique) {
		this.clique = clique;
		this.hashCode = 3;
		for (Integer i : clique) {
			hashCode *= i;
		}
		hashCode %= 10000;
	}
	
	private Set<Integer> clique;
	private int hashCode;
	
//	/**
//	 * Adds the vertex to this clique, if it is not already present.
//	 * @param v
//	 */
//	public void addVertex(Integer v) {
//		clique.add(v);
//		hashCode += v;
//		hashCode %= 10000;
//	}
	
	
	public Set<Integer> getCliqueCopy() {
		return util.Util.copySet(clique);
	}
	
	public Clique copy() {
		Clique c = new Clique(this.getCliqueCopy());
		return c;
	}


	public int size() {
		return clique.size();
	}

	/**
	 * Returns true, if the vertex calling this method
	 * knows all vertices in the clique. If the vertex
	 * @param id
	 * @param neighbors
	 * @return
	 */
	public boolean match(Set<Integer> myNeighbors) {
		for (Integer cliqueVertex : clique) {
			if (!myNeighbors.contains(cliqueVertex)) {
				return false;
			}
		}
		return true;
	}

	public String serialize(String delimitter) {
		StringBuilder s = new StringBuilder("");
		for (Integer vertex : clique) {
			s.append(vertex);
			s.append(delimitter);
		}
		return s.toString();
	}
	
	public static Clique deserialize(String serialized, String delimitter) {
		String[] s = serialized.split(delimitter);
		Set<Integer> l = new HashSet<Integer>();
		for (String str : s) {
			l.add(Integer.valueOf(str));
		}
		Clique clique = new Clique(l);
		return clique;
	}


	/**
	 * Returns true, if vertex id is in clique
	 * @param id
	 * @return
	 */
	public boolean containsVertex(Integer id) {
		if (clique.contains(id)) {
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return this.serialize("-");
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}
	
	@Override
	public boolean equals(Object o) {
		Clique c = (Clique) o;
		if (c.size()!=this.size()) {
			return false;
		} else if (c.hashCode()!=this.hashCode()) {
			return false;
		}
		else {
			for (Integer v : c.clique) {
				if (!this.clique.contains(v)) {
					return false;
				}
			}
			return true;
		}
	}
	
}
