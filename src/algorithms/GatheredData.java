package algorithms;

import algorithms.cellularAutomaton.GatheredDataCA;
import algorithms.graphIsomorphism.GatheredDataGI;
import algorithms.pageRank.GatheredDataPR;
import util.Util;

public interface GatheredData {
	
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
//	 * Deserialize the gathered data from the given string
//	 * @param string
//	 */
//	public void deserialize(String string);
}
