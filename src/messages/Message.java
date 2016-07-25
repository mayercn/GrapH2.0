package messages;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Message {

	public static final int CONSTANT_BYTE_OVERHEAD = 3 * 3 + 1;
	
	private int type;
	private int size;
	private int senderID;
	private Object payload;
	private boolean flag;
	
	/**
	 * Empty constructor for serializer
	 */
	public Message() {
		
	}
	
	public Message(int type, int payloadByteSize, Object payload) {
		this.setType(type);
		this.setSize(payloadByteSize + CONSTANT_BYTE_OVERHEAD);
		this.setPayload(payload);
		this.setSenderID(-1);
		
	}
	
	public Object getPayload() {
		return payload;
	}
	
	public void setPayload(Object payload) {
		this.payload = payload;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getSenderID() {
		return senderID;
	}

	public void setSenderID(int senderID) {
		this.senderID = senderID;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		Kryo kryo = new Kryo();
//		kryo.setReferences(false);
		
		long time = System.nanoTime();
	    // ...
	    Output output = new Output(new FileOutputStream("file.bin"));
	    List<Integer> payload = new ArrayList<Integer>();
	    for (int i=0; i<10; i++) {
	    	payload.add(i*100000);
	    }
	    Message msg = new Message(Types.BOOTSTRAP, payload.size()*4+Message.CONSTANT_BYTE_OVERHEAD, payload);
	    kryo.writeObject(output, msg);
	    kryo.writeObject(output, "hello");
	    output.close();
	    System.out.println(msg.getSize() + " bytes estimated");
	    // ...
	    Input input = new Input(new FileInputStream("file.bin"));
	    Message someObject = kryo.readObject(input, Message.class);
	    String s = kryo.readObject(input, String.class);
	    input.close();
	    System.out.println("Payload: " + (List<Integer>)someObject.getPayload());
	    System.out.println("String: " + s);
	    System.out.println("Time needed: " + (System.nanoTime() - time));
	}
}
