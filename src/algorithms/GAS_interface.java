package algorithms;

import java.io.Serializable;


import util.Edge;
import util.Vertex;



public interface GAS_interface {

	public static final int IN_EDGES = 1;
	public static final int OUT_EDGES = 2;
	public static final int ALL_EDGES = 3;
	
	/**
	 * Returns the initial vertex data of vertex with id v_id
	 * @param v_id
	 * @return
	 */
	public VertexData getVertexData(Integer v_id, VertexData oldVertexData, String vertexFile);
	
	/**
	 * Returns an empty instance of the vertex data.
	 * This is needed to be able to run different
	 * graph algorithms after each other while GrapH still
	 * knows to which type of vertex data it should deserialize.
	 * @return
	 */
	public VertexData getEmptyVertexData();
	
	/**
	 * Returns an empty instance of the gathered data.
	 * This is needed to be able to run different
	 * graph algorithms after each other while GrapH still
	 * knows to which type of gathered data it should deserialize.
	 * @return
	 */
	public GatheredData getEmptyGatheredData();
	
	
	/**
	 * Specifies whether to gather on in-edges, out-edges or all edges
	 * @return
	 */
	public int gather_edges();
	
	/**
	 * Gathers a specific neighbor
	 * @param data
	 * @return
	 */
	public GatheredData gather(Vertex myself, Vertex neighbor, Edge e);
	
	/**
	 * Sums two gathered values together
	 * @param a
	 * @param b
	 * @return
	 */
	public GatheredData sum(GatheredData a, GatheredData b);
	
	/**
	 * Changes the local vertex data based on the gathered sum and the signaling message.
	 * Also signals neighboring vertices if necessary.
	 * 
	 * @param v
	 * @param sum
	 * @param msg
	 * @return
	 */
	public void applyScatter(Vertex v, GatheredData sum);
	
}
