package util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import algorithms.GatheredData;
import algorithms.VertexData;

import modules.Partition;



public class Vertex {

	private boolean signalOutNeighbors;
	private boolean signalInNeighbors;
	
	private int vertexID;
	private List<Integer> replicaSet;
	private int master;
	private int currentTraffic;
	private int trafficPrediction;
	private boolean active;		// if a vertex is active, a GAS algorithm is currently running on it
	private VertexData data;
	private List<GatheredData> gathered;
	private boolean scheduled;
	
	// running average
	private double sum = 0;
	
	// auto-select alpha
	private double oldPrediction;
	private double oldPredictionH;
	private double oldPredictionL;
	
	private double cummError;
	private double cummErrorH;
	private double cummErrorL;
	
	private double alphaVertex;

	
	public Vertex(
			int vertexID,
			int master,
			List<Integer> replicaSet,
			VertexData data,
			int currentTraffic,
			int trafficPrediction,
			boolean scheduled) {
		this.vertexID = vertexID;
		this.replicaSet = replicaSet;
		Collections.sort(replicaSet);
		this.master = master;
		this.currentTraffic = currentTraffic;
		this.trafficPrediction = trafficPrediction;
		this.data = data;
		this.active = false;
		this.gathered = new ArrayList<GatheredData>();
		this.scheduled = scheduled;
		this.signalInNeighbors = false;
		this.signalOutNeighbors = false;
		this.alphaVertex = SystemParams.initialAlpha;
	}
	
	/**
	 * For serialization framework
	 */
	public Vertex() {
		
	}
	
	public boolean scheduled() {
		return scheduled;
	}
	
	public void setScheduled(boolean scheduled) {
		this.scheduled = scheduled;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public boolean active() {
		return this.active;
	}
	
	/**
	 * Resets the list of gathered values (e.g. if a new algorithm is started)
	 */
	public void resetGathered() {
		this.gathered = new ArrayList<GatheredData>();
	}
	
	/**
	 * Returns true, if we have a gathered value from each replica set.
	 * @return
	 */
	public boolean allGathered() {
		if (replicaSet.size()==gathered.size()) {
			return true;
		}
		return false;
	}
	
	public List<GatheredData> gathered() {
		return gathered;
	}
	
	/**
	 * Returns true, if the parameter partition is the master partition of this vertex.
	 * @param partition
	 * @return
	 */
	public boolean isMaster(int partition) {
		if (master==partition) {
			return true;
		}
		return false;
	}
	
	/**
	 * Sets the new vertex data
	 * @param data
	 */
	public void setData(VertexData data) {
		this.data = data;
	}
	
	public void setMaster(Integer id) {
		this.master = id;
	}
	
	public void setReplicaSet(List<Integer> replicaSet) {
		this.replicaSet = replicaSet;
//		System.out.println("Sorted? " + replicaSet);
//		Collections.sort(replicaSet);
	}
	
//	public VertexData dataCopy() {
//		return data.copy();
//	}
	
	/**
	 * Returns the vertex data. Note that it should be changed only with setData(...)
	 * @return
	 */
	public VertexData data() {
		return data;
	}
	
	public int id() {
		return vertexID;
	}
	
	public int master() {
		return master;
	}
	
	public List<Integer> replicaSet() {
		return replicaSet;
	}
	
	/**
	 * Sorted insert of replica into replica set
	 * @param replica
	 */
	public void addReplica(Integer replica) {
		int pos = 0;
		for (pos=0; pos<replicaSet.size(); pos++) {
			if (replicaSet.get(pos)>replica) {
				break;
			}
		}
		replicaSet.add(pos, replica);
//		System.out.println("Replica set: " + replicaSet);
	}
	
	/**
	 * Returns the traffic prediction of this vertex. TODO: prediction for each future time step.
	 * @return
	 */
	public int trafficPrediction() {
		//return 50;
		return trafficPrediction;
//		return 77;
	}
	
	/**
	 * Sets the traffic of this vertex
	 * @param traffic
	 */
	public void setTrafficPrediction(int trafficPrediction) {
		this.trafficPrediction = trafficPrediction;
	}
	
	/**
	 * Returns the current traffic value (in this superstep).
	 * @return
	 */
	public int currentTraffic() {
		return currentTraffic;
	}
	
	/**
	 * Adds the value to the current traffic statistics. Note, that updateTrafficPrediction()
	 * has to be called after all current traffic in this superstep is added.
	 */
	public void addTraffic(int traffic) {
		this.currentTraffic += traffic;
	}
	
	/**
	 * Updates the traffic prediction for the next superstep,
	 * based on the last traffic prediction and the current vertex traffic.
	 */
	public void updateTrafficPrediction_exponentialAveraging(int id, double alpha) {
		if (this.master==id) {
			// this vertex is master. Only masters should be updated, replicas copy the prediction from their masters.
			this.trafficPrediction = (int)Math.round(alpha * currentTraffic 
				+ (1.0-alpha) * this.trafficPrediction);
		}
	}
	
	public void updateTrafficPrediction_naive(int id) {
		if (this.master==id) {
			this.trafficPrediction = currentTraffic;
		}
	}
	
	public void updateTrafficPrediction_runningAverage(int id, int window) {
		if (this.master==id) {
			this.sum = this.sum + currentTraffic + this.sum / (double)window;
			this.trafficPrediction = (int)Math.round(this.sum / (double) window);
		}
	}
	
	/**
	 * Exponential averaging with automatically selecting alpha value
	 * @param partitionId
	 */
	public void updateTrafficPrediction_autoSelectAlpha(int partitionId, int iteration) {
		if (this.master==partitionId) {
			
			/*
			 * Change alpha, if it leads to lower error.
			 */
			if (iteration%SystemParams.expectedNrSupersteps == 1) {
				
				/*
				 * Update alpha to the alpha with lowest error
				 */
				if (Math.abs(cummError)>Math.abs(cummErrorH)) {
					alphaVertex = alphaVertex + SystemParams.delta;
					alphaVertex = Math.max(alphaVertex, 0);
					alphaVertex = Math.min(alphaVertex, 1.0);
//					System.out.println("New alpha: " + alphaVertex);
				} else if (Math.abs(cummError)>Math.abs(cummErrorL)) {
					alphaVertex = alphaVertex - SystemParams.delta;
					alphaVertex = Math.max(alphaVertex, 0);
					alphaVertex = Math.min(alphaVertex, 1.0);
//					System.out.println("New alpha: " + alphaVertex);
				}
				
				/*
				 * Reset error
				 */
				cummError = 0;
				cummErrorH = 0;
				cummErrorL = 0;
				
				/*
				 * Calculate new predictions based on updated alpha
				 */
				oldPrediction = (int)Math.round(alphaVertex * currentTraffic 
						+ (1.0-alphaVertex) * this.trafficPrediction);
				oldPredictionH = (int)Math.round((alphaVertex + SystemParams.delta) * currentTraffic 
						+ (1.0-(alphaVertex + SystemParams.delta)) * this.trafficPrediction);
				oldPredictionL = (int)Math.round((alphaVertex - SystemParams.delta) * currentTraffic 
						+ (1.0-(alphaVertex - SystemParams.delta)) * this.trafficPrediction);
			} else {
				cummError = this.oldPrediction - currentTraffic;
				cummErrorH = this.oldPredictionH - currentTraffic;
				cummErrorL = this.oldPredictionL - currentTraffic;
			}
			
			/*
			 * Update traffic prediction for next superstep
			 */
			trafficPrediction = (int)Math.round(alphaVertex * currentTraffic 
					+ (1.0-alphaVertex) * this.trafficPrediction);
		}
	}
	
	/**
	 * Resets the current traffic of this vertex to 0.
	 */
	public void resetCurrentTraffic() {
		currentTraffic = 0;
	}
	
	
	/**
	 * Calculates and returns the costs of this vertex for one iteration.
	 * @param topologyCosts
	 * @param traffic
	 * @return
	 */
	public double getCosts(double[][] topologyCosts) {
		double costs = 0.0;
		for (Integer p : replicaSet) {
			if (!p.equals(master)) {
				
				// costs from replicas to master
				costs += 0.5*topologyCosts[p][master];
				
				// costs from master to replicas
				costs += 0.5*topologyCosts[master][p];
			}
		}
		
		// multiply costs with real average vertex traffic per replica
		costs = costs * trafficPrediction();
		return costs;
	}

	@Override
	public boolean equals(Object o) {
		Vertex v = (Vertex) o;
		return this.id() == v.id();
	}
	
//	public String serialize() {
//		String s = "";
//		String separator = "%";
//		s += vertexID + separator;
//		s += Util.serializeList(replicaSet) + separator;
//		s += master + separator;
//		s += trafficPrediction + separator;
//		if (data==null) {
//			s += "null" + separator;
//		} else {
//			s += data.serialize() + separator;
//		}
//		s += scheduled + separator;
//		return s;
//	}
//
//	public static Vertex deserialize(String string) {
//		String[] s = string.split("%");
//		int vertexID = Integer.valueOf(s[0]);
//		List<Integer> partitions = Util.deserializeList(s[1]);
//		int master = Integer.valueOf(s[2]);
//		int traffic = Integer.valueOf(s[3]);
//		VertexData data = Partition.toExecute.get(0).getEmptyVertexData();
//		data.deserialize(s[4]);
//		boolean scheduled = Boolean.valueOf(s[5]);
//		Vertex v = new Vertex(vertexID, master, partitions, data, 0, traffic, scheduled);
//		return v;
//	}
	
	public int byteSize() {
		return 1 + 1 + 4 + replicaSet.size() * 4 + 4 + 4 + 4 + 1 + data.byteSize() + 1;
	}

	public void addToGathered(GatheredData sum) {
		if (gathered.size()==replicaSet.size()) {
			System.err.println("Problem: received gathered sum, but nothing is expected!");
			System.out.println(replicaSet);
			System.out.println(gathered);
			System.out.println("End error");
			System.exit(-1);
		}
		gathered.add(sum);
	}

	/**
	 * Sets the default master of this replica set.
	 * The master is the vertex that has the highest hash function.
	 * If the hash function gives a tie, the vertex with highest id wins.
	 */
	public void setDefaultMaster(int numberOfPartitions) {
		int tmpMaster = -1;
		int best = -1;
		for (Integer replica : replicaSet) {
			int h = hashFunction(replica, numberOfPartitions);
			if (h>best) {
				tmpMaster = replica;
				best = h;
			} else if (h==best) {
				// tie
				if (replica>tmpMaster) {
					tmpMaster = replica;
				}
			}
		}
		this.master = tmpMaster;
	}
	
	private int hashFunction(int x, int numberOfPartitions) {
		//return (vertexID*vertexID*x*x)%11;
		return (vertexID+x)%numberOfPartitions;
	}
	
	public static void main(String[] args) {
		
	}

	public boolean signalOutNeighbors() {
		return signalOutNeighbors;
	}
	
	public boolean signalInNeighbors() {
		return signalInNeighbors;
	}
	
	public void signalInNeighbors(boolean tmp) {
		signalInNeighbors = tmp;
	}
	
	public void signalOutNeighbors(boolean tmp) {
		signalOutNeighbors = tmp;
	}
	
}
