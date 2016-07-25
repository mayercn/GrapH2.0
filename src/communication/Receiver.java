package communication;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.esotericsoftware.kryo.io.Input;

import fileAccess.ReadFile;
import messages.Message;
import messages.Types;
import util.SystemParams;

/**
 * The receiver thread waits for incoming connections and starts receiverThreads
 * for communication with external machines.
 * @author marvin
 *
 */
public class Receiver extends Thread {
	
	private LinkedBlockingQueue<Message> inQ;
	private int myport;
	
	public Receiver(String machineFile, int id) {
		this.inQ = new LinkedBlockingQueue<Message>();
		// initialize myport
		List<String> lines = ReadFile.readLines(machineFile, "#");
		for (String line : lines) {
			String[] s = line.split("\\s+");
			if (s.length>0 && s[0].length()>0) {
				int tid = Integer.valueOf(s[0]);
				if (tid==id) {
					this.myport = Integer.valueOf(s[2]);
					break;
				}
			}
		}
	}
	
	/**
	 * Adds the message to the inQ to be read by the
	 * partition main thread. Note, that this method
	 * has to be thread-safe as multiple receiver threads
	 * will access it concurrently.
	 * @param msg
	 * @param sender
	 */
	public void addMessage(Message msg, int sender) {
//		System.out.println("--Receive msg " + Types.type(msg.getType()) + " from " + sender + " " + msg.getPayload());
		try {
			if (msg.getSenderID()==-1) {
				//Only for ???, the senderID could be given already.
				msg.setSenderID(sender);
			}
			inQ.put(msg);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public Message getNext() throws InterruptedException {
		return inQ.take();
	}
	
	@Override
	public void run() {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(myport);
			serverSocket.setReceiveBufferSize(SystemParams.receiveBufferSize);
			System.out.println("Server socket receive buffer size: " + serverSocket.getReceiveBufferSize());

			while (true) {
				
				/*
				 * A client is sending its worker id to the receiver thread
				 */
				Socket clientSocket = serverSocket.accept();
				BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream(), 
						SystemParams.inputBuffer);
//				InputStream in = clientSocket.getInputStream();
				Input input = new Input(in, SystemParams.inputBuffer);
				Integer id = input.readInt();
				System.out.println("-> start to receive from " + id);
				ReceiverThread receiver = new ReceiverThread(id, input, this);
				
				/*
				 * Start receiver thread to read messages from the client
				 */
				receiver.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
