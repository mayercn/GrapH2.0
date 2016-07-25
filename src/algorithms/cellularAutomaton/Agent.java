package algorithms.cellularAutomaton;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import util.Util;

public class Agent {

	// xpos-ypos-time
	public List<String> movements = new ArrayList<String>();
	
	//Initialize agent from file
	public Agent(String agentFile) {
		//movements = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(agentFile)));
			String line;
			int t = 0;
			while((line = br.readLine())!=null) {
				String[] entry = line.split("\t");				
				if(t<Integer.parseInt(entry[2]))
				{
					t = Integer.parseInt(entry[2]);
					movements.add(entry[0] + "-" + entry[1] + "-" + entry[2]);
				}
				else 
					break;
				
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public Agent() {
		
	}

	public Agent(List<String> movements) {
		this.movements = movements;
	}
	
//	public String serialize() {
//		return Util.serializeStringList(movements, "m");
//	}
//	
	public static Agent deserialize(String s) {
		List<String> movements = Util.deserializeStringList(s, "m");
		Agent a = new Agent(movements);
		return a;
	}
//	
//	public static String serializeList(List<Agent> l) {
//		String s = "";
//		Iterator<Agent> iter = l.iterator();
//		while(iter.hasNext())
//		{
//			Agent a = iter.next();
//			s += a.serialize() + ",";
//		}
//		if (s.endsWith(",")) {
//			s = s.substring(0, s.length()-1);
//		}
//		return s;
//	}
//	
	public static List<Agent> deserializeList(String s) {
		String[] split = s.split(",");
		List<Agent> l = new ArrayList<Agent>();
		for (String agent_s : split) {
			if (!agent_s.isEmpty()) {
				l.add(Agent.deserialize(agent_s));
			}
		}
		return l;
	}
}