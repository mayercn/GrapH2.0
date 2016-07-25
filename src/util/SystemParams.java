package util;

public class SystemParams {

	/*
	 * Partitioning protocol
	 */
	public static final int backOff = 20;	// number of iterations not to try a machine as exchange partner
	
//	/*
//	 * Byte sizes
//	 */
//	public static final int integerMessageSize = 5;	// bytes
	public static final int signalMessageSize = 14; // bytes
	
	/*
	 * Batch sizes
	 */
	public static final int bootstrapBatch = 10000;
//	public static final int signalBatch = 10000;
//	public static final int applyBatch = 10000;
//	public static final int gatherBatch = 10000;
	
	/*
	 * TCP send and receive buffer
	 */
	public static final int sendBufferSize = 512 * 1024;	// bytes
	public static final int receiveBufferSize = 512 * 1024;	// bytes
	
	/*
	 * In-/ Output-stream buffers
	 */
	public static final int outputBuffer = 256 * 1024;
	public static final int inputBuffer = 256 * 1024;
	
	/*
	 * Messages are sent via an outqueue. It is better to delay flushing
	 * to prevent too much operating system access. However, if flushing
	 * is delayed, the system will block because critical messages are not sent.
	 * Therefore, we regularly flush the output, if no message can be taken.
	 */
//	public static final int pollTimeoutForSender = 10;	//ms
	
	/*
	 * Partitioning
	 */
	// fraction of allowed deviation from perfect balance
	public static final double workloadDeviation = 1.3;
	
	// how many supersteps does an investment pay back?
	//(if low, than only those investment will be done that pay back within next superstep)
	public static final int expectedNrSupersteps = 3;
	public static final double initialAlpha = 0.5;
	public static final double delta = 0.2;
}
