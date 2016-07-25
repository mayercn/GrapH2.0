package communication;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import algorithms.pageRank.VertexDataPR;
import fileAccess.ReadFile;
import fileAccess.WriteToFile;
import messages.Message;
import messages.Types;
import modules.Partition;
import util.SystemParams;

public class Sender extends Thread {

	private HashMap<Integer, ConnectionInfo> connections;
	private HashMap<Integer, LinkedBlockingQueue<Message>> outQs;
	private Partition p;
	private Set<Integer> machines;
	private boolean toFlush;	// if true, the next message will result in flushing of the output
	
	public Sender(Partition p, String machineFile) {
		this.p = p;
		this.machines = new HashSet<Integer>();
		outQs = new HashMap<Integer,LinkedBlockingQueue<Message>>();
		initWorkers(machineFile);
		for (Integer i : connections.keySet()) {
			outQs.put(i, new LinkedBlockingQueue<Message>());
		}
		this.toFlush = false;
	}
	
	/**
	 * Sends the message msg to receiver.
	 * @param msg
	 * @param receiver
	 */
	public void send(Message msg, int receiver) {
//		String s1 = "-" + p.id() + " sends message: " + Types.type(msg.getType()) + " to " 
//				+ receiver + "---"+ msg.getPayload();
//		WriteToFile.writeln(p.getOutputPrefix() + "_messages.dat", true, s1);
//		System.out.println("-" + p.id() + " sends message: " + Types.type(msg.getType()) +
//				" to " + receiver + "---"+ msg.getPayload());
		if (receiver==p.id()) {
			p.receiver().addMessage(msg, receiver);
		} else {
			try {
				outQs.get(receiver).put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void run() {
		for (Integer i : connections.keySet()) {
			final int tmp = i;
			Thread thread = new Thread() {
				public void run() {
					LinkedBlockingQueue<Message> outQ = outQs.get(tmp);
					ConnectionInfo info = connections.get(tmp);
					final Output output = info.getOutput();
					Kryo kryo = new Kryo();
					kryo.register(Message.class);
					kryo.register(VertexDataPR.class);
					System.out.println("Sender: start to send to " + tmp + " outQ: " + outQ.size());
					
					/*
					 * Ensure that regularly the output is flushed
					 */
					while (true) {
						try {
//							Message msg = outQ.poll(SystemParams.pollTimeoutForSender, TimeUnit.MILLISECONDS);
//							if (msg==null) {
//								output.flush();
//								continue;
//							}
							Message msg = outQ.take();
							kryo.writeObject(output, msg);
//							if (outQ.size()==0) {
								output.flush();
//							}
//							if (info.toFlush()) {
//								output.flush();
//								info.setToFlush(false);
//							}
							
							if (msg.getType()<=16) {
								info.addPartitioningBytes(msg.getSize());
							} else {
								info.addGraphProcessingBytes(msg.getSize());
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			};
			thread.start();
		}
	}
	
//	/**
//	 * In order to flush the output in the buffer,
//	 * this function has to be called OR the buffer has to be full.
//	 */
//	public void flush() {
//		for (Integer i : connections.keySet()) {
//			connections.get(i).setToFlush(true);
//		}
//	}
	
	/**
	 * Initializes the machine connection informations (see file).
	 * Each partition has informations about each other partition.
	 * @param machineFile
	 * @return
	 */
	private void initWorkers(String machineFile) {
		System.out.println("Initizialize machines...");
		connections = new HashMap<Integer, ConnectionInfo>();
		List<String> lines = ReadFile.readLines(machineFile, "#");
		
		for (String line : lines) {
			String[] s = line.split("\\s+");
			if (s.length>0 && s[0].length()>0) {
				Socket socket = null;
				boolean connected = false;
				while (!connected) {
					try {
						int id = Integer.valueOf(s[0]);
						machines.add(id);
						InetAddress address = InetAddress.getByName(s[1]);
						int port = Integer.valueOf(s[2]);
						if (id!=p.id()) {
							socket = new Socket(address, port);
							socket.setSendBufferSize(SystemParams.sendBufferSize);
//							BufferedOutputStream outputStream =	new BufferedOutputStream(
//									socket.getOutputStream());
							OutputStream outputStream =	socket.getOutputStream();
							Output output = new Output(outputStream, SystemParams.outputBuffer);
							ConnectionInfo info = new ConnectionInfo(port, address, id, socket, output);
							connections.put(id, info);
							
							// write own id to inform machine about to whom it is connected
							System.out.println(p.id() + " connects to machine " + id);
							info.getOutput().writeInt(p.id());
							info.getOutput().flush();
							
						}
						connected = true;
					} catch (IOException e) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e1) {
						}
					}
				}
			}
		}
		System.out.println("...Machines initialized");
	}
	
	/**
	 * Returns all known machines in the machine file
	 * @return
	 */
	public Set<Integer> getWorkers() {
		return machines;
	}
	
	public int getNumberOfMachines() {
		return machines.size();
	}
	
	public Collection<ConnectionInfo> getConnectionInfos() {
		return connections.values();
	}

	/**
	 * Returns true, if the sender is connected to all machines.
	 * @return
	 */
	public boolean isConnected() {
		for (ConnectionInfo info : connections.values()) {
			if (info.getSocket()==null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns true, if any message has to be send
	 * @return
	 */
	public boolean isActive() {
		for (LinkedBlockingQueue<Message> Q : outQs.values()) {
			if (Q.size()>0) {
				return true;
			}
		}
		return false;
	}
	
//	public static void main(String[] args) {
//		Timer timer = new Timer();
//		class Task extends TimerTask { @Override public void run() { System.out.println("hello"); } }
//		timer.schedule(new Task(), 500, 100);
//	}
}
