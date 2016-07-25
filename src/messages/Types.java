package messages;

public class Types {

	// partitioning messages
	public static final int REQUEST_NEWEDGE = 13;
	public static final int REQUEST_WORKLOAD = 14;
	public static final int RESULT_WORKLOAD = 15;
	public static final int REQUEST_VERTEX = 16;
	public static final int RESULT_VERTEX_EDGE_REPLICASET = 9;
	public static final int REQUEST_CHANGEDREPLICASET = 10;
	
	// graph processing
	public static final int REQUEST_SYNCH = 17;	
	public static final int RESULT_SYNCH = 18;
	public static final int BOOTSTRAP = 19;
	public static final int REQUEST_GATHER = 20;
	public static final int RESULT_GATHER = 21;
	public static final int REQUEST_APPLY = 22;
	public static final int RESULT_APPLY = 23;
	public static final int REQUEST_SIGNAL = 24;
	public static final int RESULT_SIGNAL = 25;
	
	public static String type(int typeNumber) {
		switch (typeNumber) {
			case REQUEST_NEWEDGE: return "REQUEST_NEWEDGE";
			case REQUEST_WORKLOAD: return "REQUEST_WORKLOAD";
			case RESULT_WORKLOAD: return "RESULT_WORKLOAD";
			case REQUEST_VERTEX: return "REQUEST_VERTEX";
			case REQUEST_SYNCH: return "REQUEST_SYNCH";
			case RESULT_SYNCH: return "RESULT_SYNCH";
			case BOOTSTRAP: return "BOOTSTRAP";
			case REQUEST_GATHER: return "REQUEST_GATHER";
			case RESULT_GATHER: return "RESULT_GATHER";
			case REQUEST_APPLY: return "REQUEST_APPLY";
			case RESULT_APPLY: return "RESULT_APPLY";
			case REQUEST_SIGNAL: return "REQUEST_SIGNAL";
			case RESULT_SIGNAL: return "RESULT_SIGNAL";
			case REQUEST_CHANGEDREPLICASET: return "REQUEST_CHANGEDREPLICASET";
			case RESULT_VERTEX_EDGE_REPLICASET: return "RESULT_VERTEX_EDGE_REPLICASET";
		}
		return "NULL";
	}
	
}