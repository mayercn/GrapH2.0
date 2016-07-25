package algorithms.graphIsomorphism;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MatchingMessage {
	public int source_pos;
	public List<Integer> matched_id = new ArrayList<Integer>();	
	public List<Integer> forwarding_trace = new ArrayList<Integer>();
	
	public MatchingMessage()
	{
		source_pos = -1;
	}
	
	public MatchingMessage(MatchingMessage other)
	{
		this.matched_id = new ArrayList<Integer>(other.matched_id);
		this.source_pos = other.source_pos;
		this.forwarding_trace = new ArrayList<Integer>(other.forwarding_trace);
	}
	
	public MatchingMessage(int size_of_pattern)
	{
		matched_id = new ArrayList<Integer>(Collections.nCopies(size_of_pattern, -1));
		source_pos = -1;
	}
	
	public String serialize() 
	{
		String s = "";
		s += source_pos + ":" + util.Util.serializeList(matched_id) + ":" + util.Util.serializeList(forwarding_trace) + ":";
		return s;
	}
	
	public static MatchingMessage deserialize(String s)
	{
		MatchingMessage mm = new MatchingMessage();
		String[] split = s.split(":");
		mm.source_pos = Integer.parseInt(split[0]);
		mm.matched_id = util.Util.deserializeList(split[1]);
		mm.forwarding_trace = util.Util.deserializeList(split[2]);
		return mm;
	}
	
	public static String serializeList(List<MatchingMessage> l) {
		if(l.size()==0)
			return ",";
		String s = "";
		for (MatchingMessage mm : l) {
			s += mm.serialize() + ",";
		}
		return s;
	}
	
	public static List<MatchingMessage> deserializeList(String s) {
		String[] split = s.split(",");
		List<MatchingMessage> l = new ArrayList<MatchingMessage>();
		for (String mms : split) {
			l.add(MatchingMessage.deserialize(mms));
		}
		return l;
	}

}
