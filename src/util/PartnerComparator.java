package util;

import java.util.Comparator;
import java.util.HashMap;

import gnu.trove.iterator.TIntObjectIterator;
import modules.Partition;

public class PartnerComparator implements Comparator<Integer> {

	private HashMap<Integer, Integer> commonReplicas;
	private HashMap<Integer, Double> exchangedTraffic;
	
	public PartnerComparator(Partition p) {
		this.commonReplicas = new HashMap<Integer, Integer>();
		this.exchangedTraffic = new HashMap<Integer, Double>();
		for (Integer partitionID : p.sender().getWorkers()) {
			this.commonReplicas.put(partitionID, 0);
			this.exchangedTraffic.put(partitionID, 0.0d);
		}
//		for (Vertex v : p.subgraph().getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = p.subgraph().getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			for (Integer re : v.replicaSet()) {
				int newVal = this.commonReplicas.get(re) + 1;
				this.commonReplicas.put(re, newVal);
				
				double newTraffic = this.exchangedTraffic.get(re) + v.trafficPrediction();
				this.exchangedTraffic.put(re, newTraffic);
			}
		}
		System.out.println("Common replicas: " + commonReplicas);
		System.out.println("Exchanged traffic: " + exchangedTraffic);
	}
	
	/**
	 * Returns the number of common replicas this partition
	 * has with partition p
	 * @param p
	 * @return
	 */
	public int commonReplicas(Integer p) {
		return commonReplicas.get(p);
	}
	
	/**
	 * Returns the exchanged traffic between this partition and p
	 * @param p
	 * @return
	 */
	public double exchangedTraffic(Integer p) {
		return exchangedTraffic.get(p);
	}

//	@Override
//	public int compare(Integer arg0, Integer arg1) {
//		int commonReplicas0 = commonReplicas.get(arg0);
//		int commonReplicas1 = commonReplicas.get(arg1);
//		
//		if (commonReplicas0<commonReplicas1) {
//			return 1;
//		} else if (commonReplicas0==commonReplicas1) {
//			return 0;
//		} else {
//			return -1;
//		}
//	}
	
	@Override
	public int compare(Integer arg0, Integer arg1) {
		double traffic0 = exchangedTraffic.get(arg0);
		double traffic1 = exchangedTraffic.get(arg1);
		
		if (traffic0<traffic1) {
			return 1;
		} else if (traffic0==traffic1) {
			return 0;
		} else {
			return -1;
		}
	}

	
}
