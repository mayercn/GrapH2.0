package algorithms.cellularAutomaton;

import java.util.ArrayList;
import java.util.List;

import algorithms.GatheredData;

import util.Util;

public class GatheredDataCA implements GatheredData {

	public List<Agent> agents;
	
	public int byteSize() {
		int b = 0;
		for (Agent a : agents) {
			for (String s : a.movements) {
				b += s.getBytes().length;
			}
		}
		return b;
	}
//	
//	@Override
//	public String toString() {
//		return serialize();
//	}
//	
//	public String serialize() {
//		String s = "q";
//		String l;
//		if (agents.size()==0) {
//			l = "EMPTY";
//		} else {
//			l = Agent.serializeList(agents);
//			if (l.isEmpty()) {
//				l = "EMPTY";
//			}
//		}
//		return s + l;
//	}
//
//	public void deserialize(String string) {
//		String[] s = string.split("q");
//		if (s[0].equals("EMPTY")) {
//			this.agents = new ArrayList<Agent>();
//		} else {
//			this.agents = Agent.deserializeList(s[0]);
//		}
//	}
}
