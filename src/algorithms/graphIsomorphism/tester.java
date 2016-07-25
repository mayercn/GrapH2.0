package algorithms.graphIsomorphism;

import java.util.*;

import algorithms.graphIsomorphism.giBasic.TopologicalRelation;

public class tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*VertexDataGI vd =(VertexDataGI) new GAS_GraphIsomorphism().getVertexData(0, null);
		for(PatternVertex v: vd.local_pattern.m_pattern)
		{
			System.out.println("" + v.id + " " + v.label + " " + v.in_degree + " " + v.out_degree);
		}*/
		
		Pattern p = new Pattern("/home/licn/Desktop/test/pattern.txt", false);
		System.out.println(p.serialize());
		for(PatternVertex v: p.m_pattern)
		{
			System.out.println("" + v.id + " " + v.label + " " + v.in_degree + " " + v.out_degree + " " + v.targets.size() + " " + v.sources.size() + " " + v.neighbors.size() + " " + v.bidirected_neighbors.size());
		}

	}

}
