package algorithms.pageRank;


import algorithms.GAS_interface;
import algorithms.GatheredData;
import algorithms.VertexData;

import util.Edge;
import util.Vertex;
import modules.Partition;


public class GAS_PageRank implements GAS_interface {
	
	private Partition p;
	
	public GAS_PageRank(Partition p) {
		this.p = p;
	}

	/**
	 * Specifies whether to gather on in-edges, out-edges or all edges
	 * @return
	 */
	public int gather_edges() {
		return GAS_interface.IN_EDGES;
	}
	
	/**
	 * Gathers the vertex data from the neighbor
	 * @param neighbor
	 * @return
	 */
	@Override
	public GatheredDataPR gather(Vertex myself, Vertex neighbor, Edge e) {
		GatheredDataPR gathered = new GatheredDataPR();
		VertexDataPR neighborData = (VertexDataPR) neighbor.data();
		gathered.doubleVal = (double)neighborData.rank / (double)neighborData.outDegree; 
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
		gathered.doubleVal = ((GatheredDataPR)a).doubleVal + ((GatheredDataPR)b).doubleVal; 
		return gathered;
	}

	@Override
	public void applyScatter(Vertex v, GatheredData sum) {
		double sumGathered;
		if (sum==null) {
			sumGathered = 0;
		} else {
			sumGathered = ((GatheredDataPR) sum).doubleVal;
		}
		VertexDataPR newData = (VertexDataPR) v.data();
		double oldRank = newData.rank;
		double newRank = 0.15 + 0.85*sumGathered;
		double delta = Math.abs(oldRank - newRank);
		if (delta>0.000001) {
//		if ((p.superstep()*v.id())%p.sender().getNumberOfMachines()==0 && p.superstep()<20) {
			v.signalOutNeighbors(true);
		}
		newData.rank = newRank;
//		System.out.println("Vertex " + v.id() + ": " + oldRank + " -> " + newRank + " (gathered=" + sumG.doubleVal + ")");
	}

	@Override
	public VertexData getVertexData(Integer v_id, VertexData oldVertexData, String vertexFile) {
		VertexDataPR data = new VertexDataPR();
		data.outDegree = ((VertexDataPR)oldVertexData).outDegree;
		data.rank = 1.0;
		
//		// remove edges from graph (changing graph), but only once per GAS algorithm
//		if (!alreadyRemoved) {
//			List<Edge> toRemove = new ArrayList<Edge>();
//			for (Integer v : p.getInEdges().keySet()) {
//				for (Edge e : p.getInEdges().get(v)) {
//					if (toRemove.size()>=removeEdges) {
//						break;
//					}
//					toRemove.add(e);
//				}
//			}
//			for (Edge e : toRemove) {
//				p.removeEdge(e);
//			}
//			alreadyRemoved = true;
//		}
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
