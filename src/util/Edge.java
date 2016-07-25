package util;


public class Edge {

	public Edge(int u_id, int v_id) {
		this.u = u_id;
		this.v = v_id;
		this.hashCode = u + v;
	}
	public int u;
	public int v;
	private int hashCode;
	
	@Override
	public String toString() {
		return "( " + u + ", " + v + " )";
	}
	
	/**
	 * An edge is equal another edge, if they have the same vertices (undirected edge view)
	 */
	@Override
	public boolean equals(Object o) {
		Edge e = (Edge) o;
		if (this.u==e.u && this.v==e.v) {
			return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}

	public String serialize() {
		return u + "=" + v;
	}

	public static Edge deserialize(String string) {
		String[] s = string.split("=");
		return new Edge(Integer.valueOf(s[0]), Integer.valueOf(s[1]));
	}
}
