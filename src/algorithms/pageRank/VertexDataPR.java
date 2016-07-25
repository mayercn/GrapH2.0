package algorithms.pageRank;


import algorithms.VertexData;

public class VertexDataPR implements VertexData {
	
	public VertexDataPR() {
		
	}
	
	// PageRank
	public double rank;
	public int outDegree;
	
	@Override
	public int byteSize() {
		return 12;
	}
}
