package algorithms.outDegree;

import java.io.Serializable;
import java.util.List;

import algorithms.GAS_interface;
import algorithms.GatheredData;
import algorithms.VertexData;
import algorithms.pageRank.GatheredDataPR;
import algorithms.pageRank.VertexDataPR;

import util.Edge;
import util.Vertex;
import modules.Partition;


public class GAS_OutDegree implements GAS_interface {

	/**
	 * Specifies whether to gather on in-edges, out-edges or all edges
	 * @return
	 */
	public int gather_edges() {
		return GAS_interface.OUT_EDGES;
	}
	
	/**
	 * Gathers the vertex data from the neighbor
	 * @param neighbor
	 * @return
	 */
	@Override
	public GatheredDataPR gather(Vertex myself, Vertex neighbor, Edge e) {
		GatheredDataPR gathered = new GatheredDataPR();
		gathered.integerVal = 1;
		return gathered;
	}
	
	/**
	 * Sums two gathered objects together.
	 * @param a
	 * @param b
	 * @return
	 */
	@Override
	public GatheredDataPR sum(GatheredData a, GatheredData b) {
		GatheredDataPR gathered = new GatheredDataPR();
		gathered.integerVal = ((GatheredDataPR)a).integerVal + ((GatheredDataPR)b).integerVal; 
		return gathered;
	}

	@Override
	public void applyScatter(Vertex v, GatheredData sum) {
		VertexDataPR data = (VertexDataPR) v.data();
		if (sum==null) {
			data.outDegree = 0;
		} else {
			GatheredDataPR sumG = (GatheredDataPR) sum;
			data.outDegree = sumG.integerVal;
		}
	}

	@Override
	public VertexData getVertexData(Integer v_id, VertexData oldVertexData, String vertexFile) {
		VertexDataPR data = new VertexDataPR();
		data.outDegree = 0;
		return data;
	}

	@Override
	public VertexData getEmptyVertexData() {
		return new VertexDataPR();
	}

	@Override
	public GatheredData getEmptyGatheredData() {
		return new GatheredDataPR();
	}
}
