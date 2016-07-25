package modules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import communication.ConnectionInfo;
import communication.Receiver;
import communication.Sender;
import fileAccess.ReadFile;
import fileAccess.WriteToFile;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.set.hash.TIntHashSet;
import messages.Message;
import messages.Types;
import algorithms.GAS_interface;
import algorithms.GatheredData;
import algorithms.VertexData;
import algorithms.cellularAutomaton.GAS_CellularAutomaton;
import algorithms.clique.GAS_Clique;
import algorithms.graphIsomorphism.GAS_GraphIsomorphism;
import algorithms.graphIsomorphism.GAS_ProportionalGI2;
import algorithms.outDegree.GAS_OutDegree;
import algorithms.pageRank.GAS_PageRank;
import algorithms.pageRank.VertexDataPR;
import util.Edge;
import util.SystemParams;
import util.Util;
import util.Vertex;

/**
 * Handles the graph vertices, the main loop and the graph processing protocol
 * @author marvin
 *
 */
public class Partition extends Thread {

	public static Partition p = null;
	// parameters
	private int numberOfMachines;
	private int partitionEachSuperstep = 1;
	public final double alpha = 0.3; // importance of current traffic value for traffic prediction

	// variables
	private int superstep = 0;
	private int gasExecutions = 0;
	private long lastSuperstepTimestamp;
	private long currentLatency;
	
	private boolean partitionFlag;
	private String outputPrefix;
	private String vertexFile;
	private int id;
	public int bootstrapCounter;
	private double[][] topology;
	private boolean someMachineActive = false;

	// list of GAS algorithms to execute
	public static List<GAS_interface> toExecute = new ArrayList<GAS_interface>();
	private long startTime;
//	private int byteOverheadPartitioning = 0;
//	private long byteOverheadGraphAlgorithm = 0;

	// subgraph
	private Subgraph subgraph;
	
	// Partitioning Protocol
	private PartitioningProtocol partitioning;
	
	// sender and receiver
	private Receiver receiver;
	private Sender sender;

	// counters
	private int gatherCounter = 0;
	private int applyCounter = 0;
	private int signalCounter = 0;
	private int receivedSynchCounter = 0;

	// states
	public int state;
	public static final int BOOTSTRAP = 0;
	public static final int GATHER = 1;
	public static final int APPLY = 2;
	public static final int SIGNAL = 3;
	public static final int MIGRATE = 4;
	
	/**
	 * 
	 * @param 	args[0] : edges filename
	 * 			args[1] : partition id
	 * 			args[2] : machines filename: if each machine knows each other machine, we should call the file "allMachines.dat"
	 * 			args[3] : topology filename 
	 * 			args[4] : number of machines
	 * 			args[5] : outputPrefix
	 * 			args[6] : partitionFlag
	 * 			args[7] : vertices filename
	 * 			args[8] : the algorithm to run (PR, CA)
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public Partition(String[] args) throws NumberFormatException, IOException {
		Partition.p = this;
		startTime = System.currentTimeMillis();
		lastSuperstepTimestamp = System.nanoTime();
		id = Integer.valueOf(args[1]);
		numberOfMachines = Integer.valueOf(args[4]);
		outputPrefix = args[5];
		if (args[6].equals("0")) {
			partitionFlag = false;
			partitionEachSuperstep = 0;
		} else {
			partitionFlag = true;
			partitionEachSuperstep = Integer.valueOf(args[6]);
		}
		vertexFile = args[7];
		
		// initialize subgraph this partition is responsible for
		subgraph = new Subgraph(args[0], this);
		
		// start sender and receiver threads
		receiver = new Receiver(args[2], id);
		receiver.start();
		sender = new Sender(this, args[2]);
		sender.start();
		
		// schedule GAS algorithms
		scheduleGAS(args);

		// initialization of edges and synch replica sets
		doBootstrap();

		// initialize partitioning protocol
		partitioning = new PartitioningProtocol(this);

		// initialize vertex data
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			v.resetGathered();
			v.setActive(false);
			VertexData oldData = v.data();
			v.setData(toExecute.get(0).getVertexData(v.id(), oldData, vertexFile));
		}

		// initialize topology file
		initializeTopology(args[3]);

		System.out.println("Edges initialized");
		System.out.println("ID initialized: " + id);
		System.out.println("Topology initialized:\n" + Util.toString(topology));

		// wait until I am connected to each other machine
		waitForConnection();
		System.out.println("Machine connected to all other machines!");

	}

	/**
	 * Executes the main loop for ever
	 * @throws InterruptedException 
	 */
	public void run() {
		
		while (true) {
			Message msg = null;
			try {
				msg = receiver.getNext();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			switch (msg.getType()) {
			case Types.BOOTSTRAP:
				onBootstrap(msg);
				break;
			case Types.REQUEST_SYNCH:
				onRequestSynch(msg);
				break;
			case Types.RESULT_SYNCH:
				onResultSynch(msg);
				break;	
			case Types.REQUEST_GATHER:
				onRequestGather(msg);
				break;	
			case Types.RESULT_GATHER:
				onResultGather(msg);
				break;	
			case Types.REQUEST_APPLY:
				onRequestApply(msg);
				break;	
			case Types.RESULT_APPLY:
				onResultApply();
				break;	
			case Types.REQUEST_SIGNAL:
				onRequestSignal(msg);
				break;
			case Types.RESULT_SIGNAL:
				onResultSignal();
				break;
			//Partitioning
			case Types.REQUEST_WORKLOAD:
				partitioning.onRequestWorkload(msg.getSenderID());
				break;
			case Types.RESULT_WORKLOAD:
				partitioning.onResultWorkload(msg);
				break;
			case Types.REQUEST_CHANGEDREPLICASET:
				partitioning.onRequestChangedReplicaSet(msg);
				break;
			case Types.REQUEST_VERTEX:
				partitioning.onRequestVertex(msg);
				break;
			case Types.REQUEST_NEWEDGE:
				partitioning.onRequestNewEdge(msg);
				break;
			case Types.RESULT_VERTEX_EDGE_REPLICASET:
				partitioning.onResultVertexEdgeChangedReplicaSet();
				break;
			default:
				System.err.println("Problem: Message type not known!");
				break;
			}
		}

//			case MIGRATE:
//				if (partitioning.ready()) {
//					state = GLOBAL_SYNCH;
//					System.out.println("STATE: " + state);
//					partitioning.releaseLocks();
//				} else {
//					partitioning.execute();
//				}
//				break;
//			case GLOBAL_SYNCH:
//				if (synchReady() && allUnlocked() && !msgPending()) {
//					synch(isScheduled());
//					state = GLOBAL_SYNCH_WAIT;
//					System.out.println("STATE: " + state);
//				}
//				break;
//			case GLOBAL_SYNCH_WAIT:
//				if (synchReady() && allUnlocked()) {
//					state = IDLE;
//					System.out.println("STATE: " + state);
//				}
//				break;
//			default:
//				break;
//			}
//			processAllMsgs();
//		}
	}
	
	private void onBootstrap(Message bootstrapMsg) {
		int[] vertices = (int[]) bootstrapMsg.getPayload();
		for (int i=0; i<vertices.length; i++) {
			Integer v_id = vertices[i];
			if (subgraph.getVertices().containsKey(v_id)) {
				Vertex v = subgraph.getVertices().get(v_id);
				if (!v.replicaSet().contains(bootstrapMsg.getSenderID())) {
					v.addReplica(bootstrapMsg.getSenderID());
				}
				v.setDefaultMaster(numberOfMachines);
			}
		}
		if (bootstrapMsg.isFlag()) {
			bootstrapCounter--;
		}
		if (bootstrapCounter==0) {
			scheduleAllMasters();
			synch(true);
		}
	}

	/**
	 * Handles a request to gather all local neighbors for a vertex.
	 * @param msg
	 */
	private void onRequestGather(Message msg) {
		int v = (int) msg.getPayload();
		GatheredData sum = localGather(v);
		Object[] payload = new Object[2];
		payload[0] = v;		// vertex id
		payload[1] = sum;	// gathered sum
		int payloadSize;
		if (sum!=null) {
			payloadSize = 4 + sum.byteSize();
		} else {
			payloadSize = 4 + 1;
		}
		Message m = new Message(Types.RESULT_GATHER, payloadSize, payload);
		sender.send(m, msg.getSenderID());
	}

	/**
	 * Handles result to a gather request of
	 * a vertex for which this machine is master.
	 * @param msg
	 */
	private void onResultGather(Message msg) {
		Object[] payload = (Object[]) msg.getPayload();
		int v_id = (int) payload[0];
		Vertex v = subgraph.getVertices().get(v_id);
		
		/*
		 * Add traffic to vertex master, but normalized by replica size
		 */
		int traffic = (int) Math.round(msg.getSize() / (double) v.replicaSet().size());
		v.addTraffic(traffic);
		v.addToGathered((GatheredData) payload[1]);
		gatherCounter--;
		if (gatherCounter==0) {
			// gather phase has finished -> barrier synchronisation
			synch(true);
		}
	}

	/**
	 * Handles an apply message from the master of the vertex,
	 * i.e., a request to overwrite the vertex data (replica consistency)
	 * @param msg
	 */
	private void onRequestApply(Message msg) {
		/*
		 * Update vertex data:
		 * payload[0] = v.id();
		 * payload[1] = v.trafficPrediction();
		 * payload[2] = v.data();
		 * payload[3] = v.signalInNeighbors();
		 * payload[4] = v.signalOutNeighbors();
		 */
		Object[] payload = (Object[])msg.getPayload();
		int v_id = (int) payload[0];
		Vertex v = subgraph.getVertices().get(v_id);
		if (v!=null) {
			v.setData((VertexData) payload[2]);
			v.setTrafficPrediction((int) payload[1]);
			v.signalInNeighbors((boolean) payload[3]);
			v.signalOutNeighbors((boolean) payload[4]);
		} else {
			System.out.println("Error: can not find vertex " + v_id + " on this partition " + id);
		}
		/*
		 * Send ack to master of vertex
		 */
		Message m = new Message(Types.RESULT_APPLY, 0, null);
		sender.send(m, msg.getSenderID());
	}

	/**
	 * Handles the response to an outstanding apply request.
	 */
	private void onResultApply() {
		applyCounter--;
		if (applyCounter==0) {
			// apply phase is ready -> synchronisation barrier
			synch(true);
		}
	}

	/**
	 * Handles a request to signal a list of vertices.
	 * @param msg
	 */
	private void onRequestSignal(Message msg) {
		int signaledVertex = (int) msg.getPayload();
		Vertex v = subgraph.getVertices().get(signaledVertex);
		if (v==null) {
			System.out.println("Error: vertex not present with id " + signaledVertex);
			System.exit(-1);
		}
		if (isMaster(v)) {
			v.setScheduled(true);
			v.addTraffic(SystemParams.signalMessageSize); // 14B to send signal message
		} else {
			System.err.println("Error: received signal request for vertex, I am not master!");
			System.exit(-1);
		}
		Message m = new Message(Types.RESULT_SIGNAL, 0, null);
		sender.send(m, msg.getSenderID());
//		List<Integer> signaledVertices = (List<Integer>) msg.getPayload();
//		for (Integer v_id : signaledVertices) {
//			Vertex v = subgraph.getVertices().get(v_id);
//			if (v==null) {
//				System.out.println("Error: vertex not present with id " + v_id);
//				System.exit(-1);
//			}
//			if (isMaster(v)) {
//				v.setScheduled(true);
//				v.addTraffic(4); // 4B to send one Integer
//			} else {
//				System.err.println("Error: received signal request for vertex, I am not master!");
//				System.exit(-1);
//			}
//		}		
//		Message m = new Message(Types.RESULT_SIGNAL, 0, null);
//		sender.send(m, msg.getSenderID());
	}

	/**
	 * Handles a result to a signal and decreases
	 * counter by 1.
	 */
	private void onResultSignal() {
		signalCounter--;
		if (signalCounter==0) {
			// signal phase is ready -> barrier synchronisation
			synch(isScheduled());
		}
	}

	/**
	 * Master receives a synch request. If he has received
	 * a request from all machines, he sends a synched result
	 * to all machines (global barrier).
	 * @param msg
	 */
	private void onRequestSynch(Message msg) {
//		System.out.println("Received synch message from " + msg.senderID + " " + msg.serialize());
		receivedSynchCounter++;
		if (msg.isFlag()) {
			someMachineActive = true;
		}
		if (receivedSynchCounter==numberOfMachines) {
			receivedSynchCounter= 0;
			// send a response to all machines
			Message m = new Message(Types.RESULT_SYNCH, 0, null);
			m.setFlag(someMachineActive);
			for (Integer machine : sender.getWorkers()) {
				sender.send(m, machine);
			}
			someMachineActive = false;
		}
	}

	/**
	 * Receives response from the master machine that
	 * all machines have synched. Dependent on the state,
	 * this worker has to move forward.
	 * @param msg
	 */
	private void onResultSynch(Message msg) {

//		System.out.println("Result synch: " + msg.serialize());
//		System.out.println("Scheduled? " + isScheduled());
		/*
		 * If all machines are inactive -> switch to next algorithm or terminate execution
		 */
		if (!msg.isFlag()) {
			resetNextAlg();
		}
		
		/*
		 * Goto new state
		 */
		state = state + 1;
		if (partitionFlag && state==MIGRATE+1) {
			state = GATHER;
		}
		if (state==MIGRATE && (partitionFlag==false || superstep%partitionEachSuperstep!=0)) {
			state = GATHER;
		}
		if (state==GATHER) {
			System.out.println("GATHER");
		} else if (state==APPLY) {
			System.out.println("APPLY");
		} else if (state==SIGNAL) {
			System.out.println("SIGNAL");
		} else if (state==MIGRATE) {
			System.out.println("MIGRATE");
		} else if (state==BOOTSTRAP) {
			System.out.println("BOOTSTRAP");
		}
		
		/*
		 * Basically, each worker is a state machine with synchronisation
		 * barrier after each (!) state. Therefore, each machine is in each
		 * state at the same time.
		 */
		switch(state) {
		case GATHER:
			writeEachSuperstep();
			updateTrafficPredictions();
			resetCurrentTraffics();
			doGather();
			break;
		case APPLY:
			doApply();
			break;
		case SIGNAL:
			doSignal();
			break;
		case MIGRATE:
			doMigrate();
			break;
		default:
			break;
		}
	}

	private void doMigrate() {
		partitioning.execute();
	}

	private void doSignal() {
		
		if (signalCounter!=0) {
			System.out.println("Error: signal counter is not zero!" + signalCounter);
			System.exit(-1);
		}
		
		// schedule all signaled vertices (all neighbors are locally available)
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			List<Vertex> signaledVertices;
			if (v.signalInNeighbors() && !v.signalOutNeighbors()) {
				signaledVertices = subgraph.getInNeighbors(v);
			} else if (!v.signalInNeighbors() && v.signalOutNeighbors()) {
				signaledVertices = subgraph.getOutNeighbors(v);
			} else if (v.signalInNeighbors() && v.signalOutNeighbors()) {
				signaledVertices = subgraph.getNeighbors(v);
			} else {
				// not signaled
				continue;
			}
			for (Vertex signaledNeighbor : signaledVertices) {
				signaledNeighbor.setScheduled(true);
			}
			// reset signal out-/in-neighbors
			v.signalInNeighbors(false);
			v.signalOutNeighbors(false);
		}
		
		// forward signal requests to masters
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (v.scheduled()) {
				if (p.isMaster(v)) {
					v.addTraffic(SystemParams.signalMessageSize);
				} else {
					// signal has to be forwarded by mirror to master
					int master = v.master();
					int payload = v.id();
					Message msg = new Message(Types.REQUEST_SIGNAL, 4, payload);
					sender.send(msg, master);
					signalCounter++;
					// mirror should never be scheduled -> set to false
					v.setScheduled(false);
				}
			}
		}
		
		// reset partitioning
		if (partitionFlag) {
			partitioning.reset();
		}
		
		// if no signal was sent, synchronize already
		if (signalCounter==0) {
			synch(isScheduled());
		}
//		sender.flush();
	}

	private void doApply() {
		if (applyCounter != 0) {
			System.err.println("Cannot execute apply, there are some open apply messages!");
			System.out.println("Cannot execute apply, there are some open apply messages!");
			System.exit(-1);
		}
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (v.isMaster(id) && v.active()) {
				v.setActive(false);
				GatheredData sum = gatheredSum(v);
				if (sum != null) {
					toExecute.get(0).applyScatter(v, sum);
				} else {
					toExecute.get(0).applyScatter(v, null);
				}
				
				/*
				 * Send apply message to all replicas of v
				 */
				Object[] payload = new Object[5];
				payload[0] = v.id();
				payload[1] = v.trafficPrediction();
				payload[2] = v.data();
				payload[3] = v.signalInNeighbors();
				payload[4] = v.signalOutNeighbors();
				int payloadSize = 4+4+v.data().byteSize()+1+1;
				Message m = new Message(Types.REQUEST_APPLY, payloadSize, payload);
				int traffic = m.getSize();
				v.addTraffic(traffic);	// traffic should be independent from concrete comm.
				v.addTraffic(Message.CONSTANT_BYTE_OVERHEAD); // each apply request causes a response
				for (Integer machine : v.replicaSet()) {
					if (machine!=id) {
						sender.send(m, machine);
					}
				}
				applyCounter += v.replicaSet().size()-1;
			}
		}
		if (applyCounter==0) {
			// no apply messages sent
			// apply phase is ready -> synchronisation barrier
			synch(true);
		}
//		sender.flush();
	}

	private void doGather() {
		
		signalCounter = 0; // forget "outstanding" signal messages that were only forwarded.
		
		// track byte overhead
//		writeEachSuperstep();

		// start with next superstep
		superstep++;
		long nanoTime = System.nanoTime();
		currentLatency = nanoTime - lastSuperstepTimestamp;
		lastSuperstepTimestamp = nanoTime;
		System.out.println("Superstep: " + superstep);
		
		// gather all neighbors for which this machine is master
		if (gatherCounter!=0 || applyCounter!=0) {
			System.err.println("Can not start gathering, there are still open gather requests: " + gatherCounter);
			System.out.println("Can not start gathering, there are still open gather requests: " + gatherCounter);
			System.exit(-1);
		}
		
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			
			// execute gather, if vertex was signaled in last superstep
			if (v.scheduled() && isMaster(v)) {
				gasExecutions++;
				v.setActive(true);
				v.resetGathered();
				gatherCounter += v.replicaSet().size();
				
				// send gather request message
				int payloadSize = 4;
				Message m = new Message(Types.REQUEST_GATHER, payloadSize, v.id());
//				v.addTraffic(traffic * v.replicaSet().size());
				v.addTraffic(m.getSize());
				for (Integer machine : v.replicaSet()) {
					sender.send(m, machine);
				}
				v.setScheduled(false);
			} else if (v.scheduled() && !isMaster(v)) {
				// can be removed
				System.err.println("Error: non-master is scheduled!" + v);
				System.exit(-1);
				v.setScheduled(false);
			}
		}
		if (gatherCounter==0) {
			// no gather messages sent
			// gather phase has finished -> barrier synchronisation
			synch(true);
		}
//		sender.flush();
	}

	/**
	 * Reads edges from the partitioning file.
	 * @return
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	private void doBootstrap() {
	
		// send bootstrap vector to each machine, so that replica sets and out degrees can be initialized
		bootstrapCounter = numberOfMachines-1;
		for (Integer partition : sender.getWorkers()) {
			if (partition!=id ) {
				int i=0;
				int[] a = new int[SystemParams.bootstrapBatch];
//				for (Integer v : subgraph.getVertices().keySet()) {
				for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
					it.advance();
					Integer v = it.key();
					a[i] = v;
					i++;
					if (i==SystemParams.bootstrapBatch) {
						int payloadSize = a.length * 4;
						Message m = new Message(Types.BOOTSTRAP, payloadSize, a);
						m.setFlag(false);
						sender.send(m, partition);
						a = new int[SystemParams.bootstrapBatch];
						i = 0;
					}
				}
				int payloadSize = a.length * 4;
				Message m = new Message(Types.BOOTSTRAP, payloadSize, a);
				m.setFlag(true);
				sender.send(m, partition);
			}
		}
//		sender.flush();
	}

	/**
	 * Returns the gathered sum of all neighbors of vertex
	 * @param vertex
	 * @return
	 */
	private GatheredData localGather(Integer v_id) {
		Vertex vertex = subgraph.getVertices().get(v_id);
		if (vertex == null) {
//			System.err.println("Lost scheduling request: Can not gather vertex " + v_id + ", because not here");
			return null;
		}
		GatheredData sum = null;
		
		/*
		 * Determine edges to be gathered (depends on the GAS algorithm)
		 */
		TIntHashSet inEdgesToGather = new TIntHashSet();
		TIntHashSet outEdgesToGather = new TIntHashSet();
		switch (toExecute.get(0).gather_edges()) {
		case GAS_interface.ALL_EDGES:
			inEdgesToGather.addAll(subgraph.getInEdges(vertex));
			outEdgesToGather.addAll(subgraph.getOutEdges(vertex));
			break;
		case GAS_interface.IN_EDGES:
			inEdgesToGather.addAll(subgraph.getInEdges(vertex));
			break;
		case GAS_interface.OUT_EDGES:
			outEdgesToGather.addAll(subgraph.getOutEdges(vertex));
			break;
		default:
			System.out.println("Error: gather edges not specified correctly");
			System.exit(-1);
		}
		
		/*
		 * Gather in edges
		 */
		TIntIterator inIter = inEdgesToGather.iterator();
		while (inIter.hasNext()) {
			int inNeighbor = inIter.next();
			Vertex neighbor = subgraph.getVertices().get(inNeighbor);
			GatheredData gathered = toExecute.get(0).gather(vertex, neighbor, new Edge(inNeighbor, v_id));
			if (sum==null) {
				sum = gathered;
			} else {
				sum = toExecute.get(0).sum(sum, gathered);
			}
		}
		
		/*
		 * Gather out edges (TODO: now, a single vertex, being in- and out-neighbor, is gathered twice)
		 */
		TIntIterator outIter = outEdgesToGather.iterator();
		while (outIter.hasNext()) {
			int outNeighbor = outIter.next();
			Vertex neighbor = subgraph.getVertices().get(outNeighbor);
			GatheredData gathered = toExecute.get(0).gather(vertex, neighbor, new Edge(v_id, outNeighbor));
			if (sum==null) {
				sum = gathered;
			} else {
				sum = toExecute.get(0).sum(sum, gathered);
			}
		}
		
//		for (Edge e : edgesToGather) {
//			Vertex neighbor = subgraph.getVertices().get(Util.getOther(e.u, e.v, v_id));
//			if (neighbor==null) {
//				System.out.println("Error: vertex does not exist: " + Util.getOther(e.u, e.v, v_id));
////				System.out.println(subgraph.getInEdges().get(e.u));
////				System.out.println(subgraph.getOutEdges().get(e.u));
////				System.out.println(subgraph.getInEdges().get(e.v));
////				System.out.println(subgraph.getOutEdges().get(e.v));
//			}
//			GatheredData gathered = toExecute.get(0).gather(vertex, neighbor, e);
//			if (sum==null) {
//				sum = gathered;
//			} else {
//				sum = toExecute.get(0).sum(sum, gathered);
//			}
//		}
		return sum;
	}

	/**
	 * Calculates the gathered sum of a master vertex, that
	 * has collected gathered sums of all mirrors.
	 * @param v
	 * @return
	 */
	private GatheredData gatheredSum(Vertex v) {
		GatheredData sum = null;
		for (GatheredData gathered : v.gathered()) {
			if (sum==null) {
				sum = gathered;
			} else if (gathered!=null){
				sum = toExecute.get(0).sum(sum, gathered);
			}
		}
		return sum;
	}

	/**
	 * Sends a synchronize request to the master partition with an active flag indicating whether
	 * this partition is still running. The master partition responds only, when he received
	 * all the synch requests.
	 */
	public void synch(boolean isActive) {

		/*
		 * Send synch message. Guarantee: I am in new state if response comes
		 */
		Message m = new Message(Types.REQUEST_SYNCH, 0, null);
		m.setFlag(isActive);
		sender.send(m, getMasterMachine());
//		sender.flush();
	}

	/**
	 * Write everything to files to be written at the beginning of each superstep
	 */
	private void writeEachSuperstep() {
//		String s = superstep + "\t" + byteOverheadPartitioning
//				+ "\t" + byteOverheadGraphAlgorithm + "\t" + subgraph.getVertices().size()
//				+ "\t" + System.currentTimeMillis() + "\t" + gasExecutions;
//		WriteToFile.writeln(outputPrefix + id + "_bytes.dat", true, s);
		
		// Write latency per superstep
		String s_lat = superstep + "\t" + id + "\t" + currentLatency;
		WriteToFile.writeln(outputPrefix + "_latency.dat", true, s_lat);
		
		// Write vertex traffic + predictions for accuracy computation
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (isMaster(v)) {
				String s = superstep + "\t" + v.id() + "\t" + v.currentTraffic()
					+ "\t" + v.trafficPrediction();
				
				WriteToFile.writeln(outputPrefix + "_" + SystemParams.expectedNrSupersteps + 
						"_trafficPredictions.dat", true, s);
		
			}
		}
		
//		// Write vertex
//		for (Integer v_id : subgraph.getVertices().keySet()) {
//			Vertex v = subgraph.getVertices().get(v_id);
//			if (v.isMaster(id)) {
//				String s1 = v_id + "\t" + ((VertexDataPR)v.data()).outDegree; 
//				WriteToFile.writeln(outputPrefix + superstep + "_outDegree.dat", true, s1);
//			}
////			if (v.isMaster(id) && v.scheduled()) {
////				WriteToFile.writeln(outputPrefix + superstep + "_scheduled.dat", true, "" + v.id());
////			}
////			if (partitioning.hasLock(v)) {
////				WriteToFile.writeln(outputPrefix + superstep + "_hasLock.dat", true, v.id() + "\t" + id);
////			}
//		}
//		
//		// Write vertex replica set
//		for (Vertex v : subgraph.getVertices().values()) {
//			String s33 = v.id() + "\t9";
//			for (Integer replica : v.replicaSet()) {
//				s33+= replica + "";
//			}
//			s33+="\t" + v.master() + "\t" + this.id;
//			WriteToFile.writeln(outputPrefix + superstep + "_replication.dat", true, s33);
//		}
//		
//		// Write byte overhead
//		for (ConnectionInfo machine : sender.getConnectionInfos()) {
//			String byteOverhead = superstep + "\t" + id + "\t" + machine.id()
//					+ "\t" + machine.getPartitioningBytes() + "\t" + machine.getGraphProcessingBytes();
//			WriteToFile.writeln(outputPrefix + id + "_acrossMachineByte.dat", true, byteOverhead);
//		}
	}

	/**
	 * Returns only, if connection to all machines is established
	 */
	private void waitForConnection() {
		while (!sender.isConnected()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Connected!");
	}

	/**
	 * Schedules the GAS algorithms to be executed
	 * @param args
	 */
	private void scheduleGAS(String[] args) {
		// each machine schedules the same GAS algorithms. A new GAS algorithm is executed only after
		// the previous is ready. Here, we can schedule an arbitrary number of algorithms.
		if (args[8].equals("PR")) {
			toExecute.add(new GAS_OutDegree());
			for (int i=0; i<5; i++) {
				toExecute.add(new GAS_PageRank(this));
			}
//			toExecute.add(new GAS_OutDegree());
			System.out.println("PageRank execution scheduled");
		} else if (args[8].equals("CA")) {
			if(args.length >= 10) {
				toExecute.add(new GAS_CellularAutomaton(Integer.parseInt(args[9]), this));
			}
			else {
				for (int i=0; i<10; i++) {
					toExecute.add(new GAS_CellularAutomaton(this));
				}
			}
			System.out.println("Cellular automaton execution scheduled");
		} else if (args[8].equals("GI")) {
			boolean labeled;
			int alg_no = 1;
			if(args.length >= 10)
			{
				labeled = Boolean.valueOf(args[9]);
				try
				{
					alg_no = Integer.parseInt(args[10]);
				}
				catch(ArrayIndexOutOfBoundsException e)
				{

				}
				for(int i = 0; i < alg_no; i++)
				{
					String patternFile = args[7] + i%10;
//					String patternFile = args[7];
//					String patternFile = "/home/licn/jar_workspace/jars/pattern" + i;
					toExecute.add(new GAS_GraphIsomorphism(patternFile, labeled, this));
					System.out.println("GAS_GraphIsomorphism execution scheduled");
				}
			}
			else
			{
				System.out.println("The graph and the pattern must be specified labeled or not!");
			}
		} else if (args[8].equals("PGI2")) {

			boolean labeled;
			int alg_no = 1;
			float spread_factor = 0.01f;
			if(args.length >= 10)
			{

				labeled = Boolean.valueOf(args[9]);
				try
				{
					alg_no = Integer.parseInt(args[10]);
					spread_factor = Float.valueOf(args[11]);
				}
				catch(ArrayIndexOutOfBoundsException e)
				{

				}
				for(int i = 0; i < alg_no; i++)
				{
					String patternFile = args[7] + "pattern" + i%15;
					//							String patternFile = "/home/licn/jar_workspace/jars/pattern" + i;
					toExecute.add(new GAS_ProportionalGI2(patternFile, labeled, spread_factor, this));
					System.out.println("GAS_ProportionalGI2 execution scheduled");
				}
			}
			else
			{
				System.out.println("The graph and the pattern must be specified labeled or not!");
			}
		} else if (args[8].equals("Clique")) {
			toExecute.add(new GAS_Clique(this, Integer.valueOf(args[9])));
			System.out.println("Scheduled GAS Clique!");
		}
	}

	/**
	 * Sets current vertex traffic to zero for each vertex.
	 */
	private void resetCurrentTraffics() {
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			v.resetCurrentTraffic();
		}
	}

	/**
	 * Updates traffic predictions on each master vertex.
	 */
	private void updateTrafficPredictions() {
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (isMaster(v)) {
//				v.updateTrafficPrediction_exponentialAveraging(id, 0.5);
				v.updateTrafficPrediction_autoSelectAlpha(id, superstep);
//				v.updateTrafficPrediction_runningAverage(id, 10);
//				v.updateTrafficPrediction_naive(id);
			}
		}
	}

	/**
	 * Resets everything for the next algorithm
	 */
	private void resetNextAlg() {
		// proceed to next algorithm -> setup properly
		if (toExecute.size()>1) {
			toExecute.remove(0);
//			for (Vertex v : subgraph.getVertices().values()) {
			for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
				it.advance();
				Vertex v = it.value();
				v.resetGathered();
				v.setActive(false);
				v.setScheduled(false);
				VertexData oldData = v.data();
				v.setData(toExecute.get(0).getVertexData(v.id(), oldData, vertexFile));
			}
			System.out.println("Go to next algorithm in superstep " + superstep);
			scheduleAllMasters();
		} else {
			// write everything out and kill JVM
			flushToFile(outputPrefix);
			System.out.println("Terminated!");
			System.out.println("Total execution time (s): " + (double)(System.currentTimeMillis()-startTime)/1000.0);
			System.out.println("Vertex functions executed: " + gasExecutions);
			System.out.println("Number of local edges: " + subgraph.getNumberOfEdges());
			System.out.println("Number of local vertices: " + subgraph.getVertices().size());
			System.out.println("Workload traffic: " + subgraph.getLoad());
			while (sender.isActive()) {
				// do nothing
			}
			System.exit(-1);
		}
	}

	private void initializeTopology(String topologyFile) {
		List<String> lines = ReadFile.readLines(topologyFile, "#");
		topology = null;
		for (int i=0; i<lines.size(); i++) {
			String s[] = lines.get(i).split("\\s+");
			if (topology==null) {
				topology = new double[s.length][s.length];
			}
			for (int j=0; j<s.length; j++) {
				topology[i][j] = Double.valueOf(s[j]);
			}
		}
	}

	/**
	 * Returns false, if no local vertex is scheduled.
	 * @return
	 */
	private boolean isScheduled() {
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (v.scheduled()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Writes everything to a file with prefix filePrefix
	 * @param filePrefix
	 */
	private void flushToFile(String filePrefix) {
//		// Write latency
//		WriteToFile.writeln(filePrefix + id + "_latency.dat",  true, this.superstep + "\t" + (System.currentTimeMillis()-this.startTime));
//		
//		// Write edges
//		for (Integer v_id : inEdges.keySet()) {
//			for (Edge e : inEdges.get(v_id)) {
//				if (e.v==v_id) {
////					WriteToFile.writeln(filePrefix + id + "_edges.dat", true, e.u + "\t" + e.v);
//				}
//			}
//		}
		// Write vertex data
//		for (Integer v_id : subgraph.getVertices().keySet()) {
//			Vertex v = subgraph.getVertices().get(v_id);
//			if (v.isMaster(id)) {
//				String s = v_id + "\t" + v.trafficPrediction() + "\t" + ((VertexDataPR)v.data()).rank;
//				WriteToFile.writeln(filePrefix + "_vertices.dat", true, s);
//			}
//		}
//		// Write workload
//		int workload = 0;
//		for (Vertex v : vertices.values()) {
//			workload += v.trafficPrediction();
//		}
////		WriteToFile.writeln(filePrefix + id + "_workload.dat", true, ""+workload);
	}

	/**
	 * Schedules all master vertices once
	 */
	private void scheduleAllMasters() {
//		for (Vertex v : subgraph.getVertices().values()) {
		for ( TIntObjectIterator<Vertex> it = subgraph.getVertices().iterator(); it.hasNext(); ) {
			it.advance();
			Vertex v = it.value();
			if (isMaster(v)) {
				v.setScheduled(true);
			}
		}
	}

//	public synchronized void addByteOverheadPartitioning(int bytes) {
//		byteOverheadPartitioning+=bytes;
//	}
//
//	public synchronized void addByteOverheadGraph(int bytes) {
//		byteOverheadGraphAlgorithm+=bytes;
//	}

	public boolean amMasterMachine() {
		return id==getMasterMachine();
	}

	public int getMasterMachine() {
		return Util.getMinimal(sender.getWorkers());
	}

	public boolean isMaster(Vertex v) {
		return id==v.master();
	}

	public int id() {
		return id;
	}

	public double[][] getTopology() {
		return topology;
	}

//	public long getByteOverheadGraphAlgorithm() {
//		return byteOverheadGraphAlgorithm;
//	}

	public String getOutputPrefix() {
		return outputPrefix;
	}

//	public long getByteOverheadPartitioning() {
//		return byteOverheadPartitioning;
//	}

	public int superstep() {
		return superstep;
	}

	public void setTopology(double[][] topology) {
		this.topology = topology;
	}
	
	public Subgraph subgraph() {
		return subgraph;
	}

	public Sender sender() {
		return sender;
	}
	
	public Receiver receiver() {
		return receiver;
	}
	
	/**
	 * 
	 * @param 	args[0] : edges filename
	 * 			args[1] : partition id
	 * 			args[2] : machines filename: if each machine knows each other machine, we should call the file "allMachines.dat"
	 * 			args[3] : topology filename 
	 * 			args[4] : number of machines
	 * 			args[5] : outputPrefix
	 * 			args[6] : partitionFlag
	 * 			args[7] : vertices filename
	 * 			args[8] : the algorithm to run (PR, CA)
	 * @throws IOException 
	 * @throws NumberFormatException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {

		Partition partition = new Partition(args);
		partition.start();
		
	}

}