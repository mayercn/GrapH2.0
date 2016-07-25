package algorithms.graphIsomorphism;

import java.util.ArrayList;
import java.util.List;

import algorithms.graphIsomorphism.giBasic.TopologicalRelation;

public class MessagesInVertex {
	public int source;
	public TopologicalRelation tr;
	public List<MatchingMessage> messages = new ArrayList<MatchingMessage>();

	
	public MessagesInVertex()
	{
		source = -1;
		tr = TopologicalRelation.trUnknown;
	}
	
	public MessagesInVertex(List<MatchingMessage> m, int s, TopologicalRelation r)
	{
		messages = new ArrayList<MatchingMessage>(m);
		source = s;
		tr = r;
	}
	
	public MessagesInVertex(MessagesInVertex other)
	{
		this.messages = new ArrayList<MatchingMessage> (other.messages);
		this.source = other.source;
		this.tr = other.tr;
	}
	
	public String serialize() 
	{
		String s = "";
		s += source + ">:" + tr + ">:" + MatchingMessage.serializeList(messages) + ">:";
		return s;
	}
	
	public static MessagesInVertex deserialize(String s)
	{
		MessagesInVertex miv = new MessagesInVertex();
		String[] split = s.split(">:");
		miv.source = Integer.parseInt(split[0]);
		miv.tr = TopologicalRelation.valueOf(split[1]);
		miv.messages = MatchingMessage.deserializeList(split[2]);
		return miv;
	}
	
	public static String serializeList(List<MessagesInVertex> l) {
		if(l.size()==0)
			return ">,";
		String s = "";
		for (MessagesInVertex miv : l) {
			s += miv.serialize() + ">,";
		}
		return s;
	}
	
	public static List<MessagesInVertex> deserializeList(String s) {
		String[] split = s.split(">,");
		List<MessagesInVertex> l = new ArrayList<MessagesInVertex>();
		for (String mivs : split) {
			l.add(MessagesInVertex.deserialize(mivs));
		}
		return l;
	}

}
