package algorithms.pageRank;


import algorithms.GatheredData;


public class GatheredDataPR implements GatheredData {

	public double doubleVal = -1;	// page rank
	public int integerVal = -1;		// out degree
	
	public int byteSize() {
		return 4 + 8;
	}
//	@Override
//	public String serialize() {
//		String s = doubleVal + "q" + integerVal;
//		return s;
//	}
//	
//	@Override
//	public String toString() {
//		return serialize();
//	}
//	
//	@Override
//	public void deserialize(String string) {
//		String[] s = string.split("q");
//		this.doubleVal = Double.valueOf(s[0]);
//		this.integerVal = Integer.valueOf(s[1]);
//	}
}
