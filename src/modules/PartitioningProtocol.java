package modules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import messages.Message;
import messages.Types;
import util.BagOfEdges;
import util.Edge;
import util.PartnerComparator;
import util.SystemParams;
import util.Util;
import util.Vertex;

public class PartitioningProtocol {
	
	/*
	 * Variables
	 */
	
//	private int numberOfMigrations;
	
	
	private int openRequests = 0;
//	private Random random = new Random();
	
//	private boolean active;
	
	// the list of edge exchanges to be done
	private BagOfEdges bagOfEdges;
	
	// if a new vertex replica was created, this has to learn about
	// changed vertex replica set, too. Hence, we forward the requests
//	private HashMap<Integer, Integer> forwardReplicaSetChangesTo;	// mapping from vertex id -> partition
	
	private HashMap<Integer,Integer> backoffUntilIteration;	// mapping from partition_id --> superstep after which it can be considered as partner
	
//	private List<Integer> partnerCandidates;
	private int exchangePartner;
	private double partnerCapacity;
	private double edgeCapacity;
//	private boolean ready;
	
	private Partition p;
	
	public PartitioningProtocol(Partition p) {
//		this.partnerCandidates = new ArrayList<Integer>();
		this.p = p;
		this.backoffUntilIteration = new HashMap<Integer, Integer>();
		for (Integer machine : p.sender().getWorkers()) {
			this.backoffUntilIteration.put(machine, 0);
		}
		reset();
	}
	
	/**
	 * Resets the partitioning protocol to the initial state
	 */
	public void reset() {
		exchangePartner = -1;
		partnerCapacity = -1.0;
		edgeCapacity = -1.0;
		bagOfEdges = null;
	}
	
	/**
	 * Runs the edge exchange protocol
	 */
	public void execute() {
		
		// get Partner if partitioning has to be performed
		if (amActive()) {
			
			// be active and find partner
			exchangePartner = getPartner();
			if (exchangePartner<0) {
				// be passive and wait for partner requests
				System.out.println("Be passive partitioning!");
				finish();
				return;
			}
			System.out.println(p.id() + " active migration to " + exchangePartner);
			
			// send workload message
			Message m = new Message(Types.REQUEST_WORKLOAD, 0, null);
			p.sender().send(m, exchangePartner);
		} else {
			// be passive and wait for partner requests
			System.out.println("Be passive partitioning!");
			finish();
			return;
		}
	}
	
	/**
	 * Sends the vertex updates, replica set updates, and edge updates.
	 */
	private void implementBag() {
		
		/*
		 * Send vertices and replica sets
		 */
		for (Vertex v : bagOfEdges.newReplicaSets) {
			Vertex oldV = p.subgraph().getVertices().get(v.id());

			/*
			 * Exchange partner learns the first time about the vertex
			 * -> send the vertex to exchange partner
			 */
			if (!oldV.replicaSet().contains(exchangePartner)) {
				Message m = new Message(Types.REQUEST_VERTEX, v.byteSize(), v);
				p.sender().send(m, exchangePartner);
				openRequests++;
			}

			/*
			 * As newReplicaSet stores only the vertices that have changed
			 * replica sets, we need to send a notify message to each replica.
			 */
			Object[] payload = new Object[4];
			payload[0] = v.id();
			payload[1] = v.master();
			payload[2] = Util.copyList(v.replicaSet());
			if (!v.replicaSet().contains(p.id()) && p.isMaster(oldV)) {
				payload[3] = v.scheduled();
			} else {
				payload[3] = false;
			}
			Message m = new Message(Types.REQUEST_CHANGEDREPLICASET, 0, payload);
			for (Integer replica : v.replicaSet()) {
				p.sender().send(m, replica);
				openRequests++;
			}

			/*
			 * Remove vertex on this partition
			 */
			if (!v.replicaSet().contains(p.id())) {
				if (p.subgraph().getInEdges(v).size()>0 || p.subgraph().getOutEdges(v).size()>0) {
					System.out.println("Error in partitioning protocol: remove vertex that has local edges!");
					System.exit(-1);
				}
				p.subgraph().removeVertex(v.id());
			}
		}

		/*
		 *  Send edges
		 */
		for (Edge e : bagOfEdges.edgesToSend) {
			int[] payload = new int[2];
			payload[0] = e.u;
			payload[1] = e.v;
			Message m = new Message(Types.REQUEST_NEWEDGE, 0, payload);
			p.sender().send(m, exchangePartner);
			openRequests++;
		}
		if (openRequests==0) {
			System.out.println("Empty bag, no migration was performed!");
			finish();
		}
	}
	
	

	/**
	 * Adds all edges in the parameter to the p.
	 * @param edgesToSend
	 */
	private void addAllEdges(Set<Edge> edgesToSend) {
		for (Edge e : edgesToSend) {
			p.subgraph().newEdge(e);
		}
	}
	

	/**
	 * Each partition is active only each second superstep.
	 * @return
	 */
	private boolean amActive() {
		return true;
//		if (numberOfMigrations>0) {
//			active = random.nextBoolean();
//			return active;
//		}
//		return false;
	}

	/**
	 * Quick heuristic for bag of edges.
	 * @return
	 */
	private BagOfEdges initExchangeCandidates() {
		
		// Consider only adjacent vertices
		List<Vertex> vertexCandidates = new ArrayList<Vertex>();
//		for (Vertex v : p.subgraph().getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = p.subgraph().getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (v.replicaSet().contains(exchangePartner) && hasLock(v)) {
				vertexCandidates.add(v);
			}
		}		
		
		List<BagOfEdges> bagList = new ArrayList<BagOfEdges>();
		if (vertexCandidates.size()>0) {
			Util.sortByVertexTraffic(vertexCandidates);
			System.out.println("Start edge migration; top traffic vertex: " + vertexCandidates.get(0).trafficPrediction());
			double cap = partnerCapacity;
			for (Vertex v : vertexCandidates) {
				if (cap<0 ) {
					break;
				}
				Set<Edge> tmpEdges = new HashSet<Edge>();
				
				/*
				 * Add all in edges
				 */
				TIntIterator iter2 = p.subgraph().getInEdges(v).iterator();
				while (iter2.hasNext()) {
					Edge inEdge = new Edge(iter2.next(),v.id());
					tmpEdges.add(inEdge);
				}
				
				/*
				 * Add all out edges
				 */
				iter2 = p.subgraph().getOutEdges(v).iterator();
				while (iter2.hasNext()) {
					Edge outEdge = new Edge(v.id(), iter2.next());
					tmpEdges.add(outEdge);
				}
				
				// consider only edges where lock on both endpoints are held
				Iterator<Edge> iter = tmpEdges.iterator();
				while (iter.hasNext()) {
					Edge e = iter.next();
					if (!hasLock(p.subgraph().getVertices().get(e.u)) ||
							!hasLock(p.subgraph().getVertices().get(e.v))) {
						iter.remove();
					}
				}
 				
				// remove all edges to send
				for (Edge e : tmpEdges) {
					p.subgraph().removeEdge(e);
				}
				List<Vertex> tmpVertices = BagOfEdges.getNewVerticesAfterSending(tmpEdges, exchangePartner, p);
				double tmpCosts = BagOfEdges.getCostChange(tmpVertices,
						tmpEdges, exchangePartner, SystemParams.expectedNrSupersteps, p);
				if (tmpCosts<0) {
					double oldCap = cap;
					for (Vertex newV : tmpVertices) {
						Vertex oldV = p.subgraph().getVertices().get(newV.id());
						if (!oldV.replicaSet().contains(exchangePartner)) {
							cap -= newV.trafficPrediction();
						}
					}
//					if (cap<0) {
//						// skip migration of this vertex because it would result in imbalances
//						addAllEdges(tmpEdges);
//						cap = oldCap;
//					} else {
						BagOfEdges bag = new BagOfEdges(tmpVertices, tmpEdges, tmpCosts);
						bagList.add(bag);
//					}
				} else {
					addAllEdges(tmpEdges);
				}
			}
		}
		
		// Now we have a list of bag of edges, which individual exchange results in lower costs
		// if we send the union, this results in lower costs, too.
		Set<Edge> unionEdges = new HashSet<Edge>();
		List<Vertex> unionVertices;
		double totalCostDelta = 0;
		for (BagOfEdges bag : bagList) {
			for (Edge e : bag.edgesToSend) {
				unionEdges.add(e);
			}
//			for (Vertex v : bag.newReplicaSets) {
//				// the newer vertex version replaces the older one.
//				unionVertices.put(v.id(), v);
//			}
			totalCostDelta += bag.costChange;
		}
		unionVertices = BagOfEdges.getNewVerticesAfterSending(unionEdges, exchangePartner, p);
		BagOfEdges unionBag = new BagOfEdges(unionVertices, unionEdges, totalCostDelta);
		System.out.println("Bag of edges: " + unionBag.edgesToSend.size() + " edges, delta costs: " + unionBag.costChange);
		return unionBag;
	}
	
	/**
	 * Determines, whether partition p has lock for v
	 * in this iteration. TODO: more sophisticated approach. E.g., if we know that no partition
	 * will modify this vertex, we can assume that we have lock.
	 * @param v
	 * @return
	 */
	public boolean hasLock(Vertex v) {
//		if (p.isMaster(v)) {
		/*
		 * Default: one partition has all locks it needs
		 */
		int partitionHasLock = p.superstep() % p.sender().getNumberOfMachines();
		if (v.replicaSet().contains(partitionHasLock)) {
			if (p.id()==partitionHasLock) {
				return true;
			} else {
				return false;
			}
		} else {
			/*
			 * Default partition does not need this lock.
			 * Give it to the partition in the replica set with
			 */
			List<Integer> replicaSet = v.replicaSet();
			partitionHasLock = replicaSet.get(p.superstep()%replicaSet.size());
			if (partitionHasLock==p.id()) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	/**
	 * 
	 * Returns the partner partition id with which edges should be exchanged.
	 * @return
	 */
	public int getPartner() {
		List<Integer> candidates = new ArrayList<Integer>(p.sender().getWorkers());
		PartnerComparator comparator = new PartnerComparator(p);
		
		/*
		 * Remove candidates that have no exchanged traffic, because
		 * no improvement can be found. Remove also candidates that are currently ignored.
		 */
		Iterator<Integer> iter = candidates.iterator();
		while (iter.hasNext()) {
			Integer tmpPartner = iter.next();
			if (comparator.exchangedTraffic(tmpPartner)<=0.1) {
				iter.remove();
			}
			if (p.superstep()<backoffUntilIteration.get(tmpPartner)) {
				iter.remove();
			}
		}
		
		/*
		 * Sort candidates by exchanged traffic
		 */
		Collections.sort(candidates, comparator);
		System.out.println("Partner candidates (sorted): " + candidates);
		if (candidates.size()==0) {
			return -1;
		} else {
			backoffUntilIteration.put(candidates.get(0), p.superstep()+SystemParams.backOff);
			return candidates.get(0);
		}
		
//		if (partnerCandidates.size()==0) {
//			partnerCandidates = new ArrayList<Integer>(p.sender().getWorkers());
//			partnerCandidates.remove(new Integer(p.id()));
//			PartnerComparator comparator = new PartnerComparator(p);
//			Collections.sort(partnerCandidates, comparator);
////			System.out.println("Partner candidates (sorted): " + partnerCandidates);
//		}
////		List<Integer> candidates = new ArrayList<Integer>(p.getPartitions().keySet());
////		candidates.remove(new Integer(p.getId()));
//		return partnerCandidates.get(0);
	}

	/**
	 * Receives a request from machine partition_id for the workload
	 * @param partition_id
	 */
	public void onRequestWorkload(int partition_id) {
		int workload;
		int numberOfLocalEdges;
		// conditions
//		if (active || exchangePartner>=0) {
//			// reject partitioning request
//			workload = -1;
//			numberOfLocalEdges = 0;
//		} else {
			// start to serve requesting partition
			workload = p.subgraph().getLoad();
//			workload = p.subgraph().getNumberOfEdges();
			numberOfLocalEdges = p.subgraph().getNumberOfEdges();
//			exchangePartner = partition_id;
//		}
		
		// send response workload
		int[] payload = new int[2];
		payload[0] = workload;
		payload[1] = numberOfLocalEdges;
		int payloadSize = 4 + 4;
		Message m = new Message(Types.RESULT_WORKLOAD, payloadSize, payload);
		p.sender().send(m, partition_id);
	}
	
	/**
	 * Receives workload info from partner partition_id
	 * @param load
	 * @param partition_id
	 */
	public void onResultWorkload(Message msg) {
		int[] payload = (int[]) msg.getPayload();
		int load = payload[0];
//		int numberOfEdges = msg.integer2;
		int partition_id = msg.getSenderID();
		if (exchangePartner!=partition_id) {
			System.err.println("Error: received result for workload request from partition " + partition_id);
			System.err.println("Should be from partition " + exchangePartner);
			return;
		}
		
		// Now, we have a response from exchangePartner, we can remove it from the candidate list.
//		partnerCandidates.remove(0);
		
		if (load<0) {
			// remote machine has rejected repartitioning request, because it is busy
			System.out.println(partition_id + " has rejected repartitioning request!");
//			partnerCandidates.add(0, partition_id);
			finish();
		} else {
			double maxLoad = Math.min(p.subgraph().getLoad(),load) * (1.0+SystemParams.workloadDeviation);
			partnerCapacity = maxLoad - load;
//			edgeCapacity = (p.getNumberOfEdges()-numberOfEdges)/2.0 + (numberOfEdges * workloadDeviation);
//			System.out.println(p.getId() + ": New exchange partner found (" 
//						+ exchangePartner + ") with capacity " + partnerCapacity);
//			if (partnerCapacity<=0 || edgeCapacity<=0 || load>p.getLoad()) {
			if (partnerCapacity<=0) {
				System.out.println("Partner capacity below zero. Cannot migrate!");
				finish();
			} else {
				// the partner has capacity to receive edges -> initiate the repartitioning
				
				/*
				 * What is the dream bag-of-edges?
				 */
				bagOfEdges = initExchangeCandidates();
				
				/*
				 * Send locks, so that bag-of-edges can be implemented
				 */
				if (bagOfEdges.costChange<0) {
					System.out.println("Migration: " + p.id() + " -> " + exchangePartner + " (" + partnerCapacity 
							+ "traffic , " + edgeCapacity + " edges)" );
//					System.out.println(bagOfEdges.edgesToSend);
//					System.out.println(p.subgraph().toString());
					implementBag();
//					System.out.println(p.subgraph().toString());
				} else {
					addAllEdges(bagOfEdges.edgesToSend);
					bagOfEdges = null;
					finish();
					return;
				}
			}
		}
	}
	
	/**
	 * Synchronizes with the master machine
	 * and resets this partitioning 
	 * @param r
	 */
	private void finish() {
		p.synch(true);
//		reset();
	}
	
	/**
	 * Changes a vertex as specified in the parameter.
	 * @param integer2
	 */
	public void onRequestVertex(Message msg) {
		
		Vertex newVertex = (Vertex) msg.getPayload();
		
		Vertex oldVertex = p.subgraph().getVertices().get(newVertex.id());
		if (oldVertex!=null) {
			System.err.println("Error: vertex should not exist on this partition: " + oldVertex);
			System.exit(-1);
		}
		
		if (p.isMaster(newVertex) && newVertex.scheduled()) {
			if (newVertex.replicaSet().contains(msg.getSenderID())) {
				System.err.println("Error: scheduling");
				System.exit(-1);
			}
			newVertex.setScheduled(true);
		} else {
			newVertex.setScheduled(false);
		}
		
		p.subgraph().getVertices().put(newVertex.id(), newVertex);

		// send a result to the vertex request
		Message m = new Message(Types.RESULT_VERTEX_EDGE_REPLICASET, 0, null);
		p.sender().send(m, msg.getSenderID());
	}
	
	/**
	 * Create a new edge on this p. The specified vertices have to be
	 * already present on this p.
	 * @param id1
	 * @param id2
	 */
	public void onRequestNewEdge(Message msg) {
		int[] payload = (int[])	msg.getPayload();
		int id1 = (int) payload[0];
		int id2 = (int) payload[1];
		
		if (!p.subgraph().getVertices().containsKey(id1)) {
			System.err.println("Could not add edge " + id1 + ", " + id2 + ", because " + id1 + " not here!");
			return;
		}
		if (!p.subgraph().getVertices().containsKey(id2)) {
			System.err.println("Could not add edge " + id1 + ", " + id2 + ", because " + id2 + " not here!");
			return;
		}
		
		// integrate edge
		Edge e = new Edge(id1,id2);
		p.subgraph().newEdge(e);
//		System.out.println("-----------");
//		System.out.println(p.subgraph().toString());
//		System.out.println(".............");
		
		// send a result to the vertex request
		Message m = new Message(Types.RESULT_VERTEX_EDGE_REPLICASET, 0, null);
		p.sender().send(m, msg.getSenderID());
	}

	/**
	 * Is called, when an edge or vertex sending request has been received
	 */
	public void onResultVertexEdgeChangedReplicaSet() {
		openRequests--;
		if (openRequests==0) {
			System.out.println("No outstanding request. Migration ready!");
			finish();
		}
	}

	/**
	 * Handles the request to a changed replica set.
	 * @param msg
	 */
	public void onRequestChangedReplicaSet(Message msg) {
		Object[] payload = (Object[]) msg.getPayload();
		int v_id = (int) payload[0];
		Vertex v = p.subgraph().getVertices().get(v_id);
		if (v!=null) {
			v.setReplicaSet((List<Integer>) payload[2]);
			v.setMaster((int) payload[1]);
			if (p.isMaster(v)) {
				v.setScheduled((boolean) payload[3] || v.scheduled());
			} else {
				v.setScheduled(false);
			}
		}
		
		// send a result to the replica set request
		Message m = new Message(Types.RESULT_VERTEX_EDGE_REPLICASET, 0, null);
		p.sender().send(m, msg.getSenderID());
	}
	
}
