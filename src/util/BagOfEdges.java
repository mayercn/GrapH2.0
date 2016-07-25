package util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import messages.Message;
import messages.Types;
import modules.Partition;

public class BagOfEdges {
	
	public List<Vertex> newReplicaSets;
	public Set<Edge> edgesToSend;
	public double costChange;

	public BagOfEdges(List<Vertex> newReplicaSets, Set<Edge> edgesToSend, double costChange) {
		this.newReplicaSets = newReplicaSets;
		this.edgesToSend = edgesToSend;
		this.costChange = costChange;
	}
	
	
	public String toString() {
		return "new replica sets: " + newReplicaSets + "\nedges to send: "
				+ edgesToSend + "\ncost change: " + costChange;
	}

	/**
	 * Returns the expected cost change of implementing the new vertex replica sets and sending the edges
	 * @param tmpVertices
	 * @param tmpEdges
	 * @return
	 */
	public static double getCostChange(
			List<Vertex> newVertices,
			Set<Edge> tmpEdges,
			int exchangePartner,
			int expectedNrSupersteps,
			Partition p) {
		double delta = 0.0;
		List<Vertex> oldVertices = new ArrayList<Vertex>();
		for (Vertex v : newVertices) {
			oldVertices.add(p.subgraph().getVertices().get(v.id()));
		}
//		System.out.println(tmpEdges);
//		System.out.println("New vertices: " + newVertices + "----------" + oldVertices);
		delta = BagOfEdges.getCosts(newVertices, p.getTopology())-BagOfEdges.getCosts(oldVertices, p.getTopology());
		return delta * expectedNrSupersteps 
				+ BagOfEdges.getInvestmentCosts(newVertices, tmpEdges, exchangePartner, p.id(), p.getTopology());
	}
	
	/**
	 * Returns the costs of implementing this bag of edges.
	 * @return
	 */
	public static double getInvestmentCosts(List<Vertex> newVertices, Set<Edge> tmpEdges,
			int exchangePartner, int myselfID, double[][] topology) {
		double costs = 0.0;
		
		// send vertices
		for (Vertex v : newVertices) {
			int messageBytes = v.byteSize() + Message.CONSTANT_BYTE_OVERHEAD;
			for (int partition : v.replicaSet()) {
				// update all machines with new vertex
				costs += messageBytes * topology[myselfID][partition];
			}
		}
		
//		// heuristic
//		if (newVertices.size()>0) {
//			Message m = new Message(Types.REQUEST_VERTEX, 0, );
//			m.type = Types.REQUEST_VERTEX;
//			Vertex v = newVertices.get(0);
//			int messageBytes = v.byteSize() + Message.CONSTANT_BYTE_OVERHEAD;
////			System.out.println("Bytes per vertex message: " + messageBytes);
//			for (int partition : v.replicaSet()) {
//				// update all machines with new vertex
//				costs += messageBytes * topology[myselfID][partition];
//			}
//			costs *= newVertices.size();
//		}
		
		// send edges
		int messageBytes = 4 + 4 + Message.CONSTANT_BYTE_OVERHEAD;
//		System.out.println("Bytes per edge message: " + m2.serialize().getBytes().length);
		costs += tmpEdges.size() * messageBytes
				* topology[myselfID][exchangePartner];
		return costs;
	}	
	
	/**
	 * Returns the costs induced by these specific replica sets.
	 * @param vertices
	 * @return
	 */
	private static double getCosts(List<Vertex> vertices, double[][] topology) {
		double costs = 0.0;
		for (Vertex v : vertices) {
			costs += v.getCosts(topology);
		}
		return costs;
	}


//	/**
//	 * Returns the replica sets of all adjacent vertices after theoretical sending tmpEdges to exchangePartner
//	 * @param tmpEdges
//	 * @param exchangePartner
//	 * @return
//	 */
//	public static List<Vertex> getVerticesAfterSending2(Set<Edge> tmpEdges,
//			int exchangePartner) {
//		
//		// initialize in- and out-edges of all affected vertices
//		HashMap<Integer, List<Edge>> inEdges = new HashMap<Integer, List<Edge>>();
//		HashMap<Integer, List<Edge>> outEdges = new HashMap<Integer, List<Edge>>();
//		for (Edge e : tmpEdges) {
//			Vertex eu = Partition.getVertices().get(e.u);
//			Vertex ev = Partition.getVertices().get(e.v);
//			if (!inEdges.containsKey(e.u)) {
//				inEdges.put(e.u, Util.copyEdgeList(Partition.getInEdges(eu)));
//			}
//			if (!inEdges.containsKey(e.v)) {
//				inEdges.put(e.v, Util.copyEdgeList(Partition.getInEdges(ev)));
//			}
//			if (!outEdges.containsKey(e.u)) {
//				outEdges.put(e.u, Util.copyEdgeList(Partition.getOutEdges(eu)));
//			}
//			if (!outEdges.containsKey(e.v)) {
//				outEdges.put(e.v, Util.copyEdgeList(Partition.getOutEdges(ev)));
//			}
//		}
//		
//		// remove all edges to be send
//		for (Edge e : tmpEdges) {
//			inEdges.get(e.v).remove(e);
//			outEdges.get(e.u).remove(e);
//		}
//		
//		// construct new vertex replica sets (some vertices might be isolated)
//		Set<Integer> ids = new HashSet<Integer>(inEdges.keySet());
//		ids.addAll(outEdges.keySet());
//		List<Vertex> l = new ArrayList<Vertex>();
//		for (Integer v_id : ids) {
//			Vertex v = Partition.getVertices().get(v_id);
//			Vertex newV = new Vertex(v.id(), v.master(), Util.copyList(v.replicaSet()), v.data(), v.locked(),
//					v.currentTraffic(), v.trafficPrediction(), v.scheduled());
////			Vertex newV = v.copy();
//			newV.lock();	// the new vertex is locked, maybe there is a new master
//			if ((inEdges.get(v_id)==null || inEdges.get(v_id).size()==0)
//					&& (outEdges.get(v_id)==null || outEdges.get(v_id).size()==0)) {
//				// after sending the edges, this vertex will be isolated and can be removed
//				newV.replicaSet().remove(new Integer(Partition.getId()));
//				if (newV.master()==Partition.getId()) {
//					newV.setMaster(exchangePartner);
//				}
//			}
//			if (!newV.replicaSet().contains(new Integer(exchangePartner))) {
//				newV.replicaSet().add(exchangePartner);
//			}
//			l.add(newV);
//		}
//		//TODO: consider master placement
//		return l;
//	}
	
	/**
	 * Returns only the changed vertex replica sets after tmpEdges have been sent to exchangePartner.
	 * @param tmpEdges
	 * @param exchangePartner
	 * @return
	 */
	public static List<Vertex> getNewVerticesAfterSending(Set<Edge> tmpEdges,
			int exchangePartner, Partition p) {
		
		// construct new vertex replica sets of all vertices that change after edges were sent
		HashMap<Integer,Vertex> newVertices = new HashMap<Integer, Vertex>();
		for (Edge e : tmpEdges) {
			List<Integer> edgeIDs = new ArrayList<Integer>();
			edgeIDs.add(e.u);
			edgeIDs.add(e.v);
			for (Integer id : edgeIDs) {
				if (!newVertices.containsKey(id)) {
					Vertex v = p.subgraph().getVertices().get(id);
					Vertex newV = new Vertex(v.id(), v.master(), Util.copyList(v.replicaSet()), v.data(),
							v.currentTraffic(), v.trafficPrediction(), v.scheduled());
					boolean changed = false;
					if (p.subgraph().getInEdges(v).size()==0
							&& p.subgraph().getOutEdges(v).size()==0) {
						// after sending the edges, this vertex will be isolated and can be removed
						newV.replicaSet().remove(new Integer(p.id()));
						changed = true;
						if (newV.master()==p.id()) {
							newV.setMaster(exchangePartner);
						}
					}
					if (!newV.replicaSet().contains(exchangePartner)) {
						newV.addReplica(exchangePartner);
						changed = true;
					}
					if (changed) {
//						System.out.println("Old replica set: " + v.replicaSet() + "\t" + v.master());
//						System.out.println("New replica set: " + newV.replicaSet() + "\t" + newV.master());
						newVertices.put(newV.id(), newV);
					}
				}
			}
		}
		return new ArrayList<Vertex>( newVertices.values() );
	}
	
}
