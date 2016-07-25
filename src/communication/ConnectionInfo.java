package communication;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

import com.esotericsoftware.kryo.io.Output;

public class ConnectionInfo {

	private int port;
	private InetAddress address;
	private Socket socket;
	private Output output;
	private int graphProcessingBytes;
	private int partitioningBytes;
	private int id;
	
	public ConnectionInfo(int port, InetAddress address, int id, Socket socket, Output output) {
		this.port = port;
		this.address = address;
		this.socket = socket;
		this.output = output;
		this.graphProcessingBytes = 0;
		this.partitioningBytes = 0;
		this.id = id;
	}
	
	public int id() {
		return id;
	}
	
	public int getPort() {
		return port;
	}
	public InetAddress getInetAddress() {
		return address;
	}
	public Socket getSocket() {
		return socket;
	}
//	public BufferedWriter getOut() {
//		return out;
//	}
	
	public Output getOutput() {
		return output;
	}
	
	public int getPartitioningBytes() {
		return partitioningBytes;
	}
	
	public int getGraphProcessingBytes() {
		return graphProcessingBytes;
	}
	
	public void addPartitioningBytes(int bytes) {
		partitioningBytes += bytes;
	}
	
	public void addGraphProcessingBytes(int bytes) {
		graphProcessingBytes += bytes;
	}

}
