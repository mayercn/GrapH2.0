package algorithms;


public interface VertexData {
	
	/*
	 *  Returns the current size of the vertex data in bytes.
	 *  This is needed to calculate the vertex traffic weights.
	 */
	public int byteSize();
	
//	public String toString();
//	
//	/**
//	 * Returns the gathered data as serialized string
//	 * that can be sent over the network.
//	 * @return
//	 */
//	public String serialize();
//
//	/**
//	 * Deserialize the vertex data from the given string
//	 * @param string
//	 */
//	public void deserialize(String string);
//	
//	public VertexData copy();
//
//	/**
//	 * Define a custom initialization of the variables in vertex data.
//	 */
////	public void initialize() {
////	}
}
