package communication;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import algorithms.pageRank.VertexDataPR;
import messages.Message;
import messages.Types;


public class ReceiverThread extends Thread {

	private int sender_id;
	private Receiver receiver;
	private Input input;
	private Kryo kryo;
	
	public ReceiverThread(int id, Input input, Receiver receiver) {
		this.sender_id = id;
		this.receiver = receiver;
		this.input = input;
		this.kryo = new Kryo();
		this.kryo.register(Message.class);
		this.kryo.register(VertexDataPR.class);
		// TODO: register all VertexData classes
	}
	
	@Override
	public void run() {
		while (true) {
			Message msg = kryo.readObject(input, Message.class);
			receiver.addMessage(msg, sender_id);
		}
	}
	
}
