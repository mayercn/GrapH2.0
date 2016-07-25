package algorithms.graphIsomorphism;

import java.util.ArrayList;
import java.util.List;

import algorithms.GatheredData;

public class GatheredDataGI implements GatheredData {
	
	public List<MessagesInVertex> gathered_messages = new ArrayList<MessagesInVertex>();	
	
	@Override
	public int byteSize() {
		int b = 0;
		for (MessagesInVertex m : gathered_messages) {
			for (MatchingMessage msg : m.messages) {
				b += 4;
				b += msg.forwarding_trace.size() * 4;
				b *= msg.matched_id.size() * 4;
			}
			b += 4;
		}
		return b;
	}

}
