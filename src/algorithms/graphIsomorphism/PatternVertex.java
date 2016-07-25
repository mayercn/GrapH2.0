package algorithms.graphIsomorphism;

import java.util.*;

public class PatternVertex {
	public int id;
	public String label;
	public int in_degree;
	public int out_degree;
	public List<Integer> targets = new ArrayList<Integer>();
	public List<Integer> sources = new ArrayList<Integer>();
	public List<Integer> neighbors = new ArrayList<Integer>();
	public List<Integer> bidirected_neighbors = new ArrayList<Integer>();

	public PatternVertex()
	{
		this.id = -1;
		in_degree = 0;
		out_degree = 0;
	}
	
	public PatternVertex(int id, String label)
	{
		this.id = id;
		this.label = label;
		in_degree = 0;
		out_degree = 0;
	}
	
	public String serialize() 
	{
		String s = "";
		s += id + ":" + label + ":" + in_degree + ":" + out_degree + ":";
		s += util.Util.serializeList(targets) + ":" + util.Util.serializeList(sources) + ":" + util.Util.serializeList(neighbors) + ":" + util.Util.serializeList(bidirected_neighbors) + ":";
		return s;
	}
	
	public static PatternVertex deserialize(String s)
	{
		PatternVertex pv = new PatternVertex();
		String[] split = s.split(":");
		pv.id = Integer.parseInt(split[0]);
		pv.label = split[1];
		pv.in_degree = Integer.parseInt(split[2]);
		pv.out_degree= Integer.parseInt(split[3]);
		pv.targets = util.Util.deserializeList(split[4]);
		pv.sources = util.Util.deserializeList(split[5]);
		pv.neighbors = util.Util.deserializeList(split[6]);
		pv.bidirected_neighbors = util.Util.deserializeList(split[7]);
		return pv;
	}
	
	public boolean AddTarget(int t)
	{
		for (int i : targets)
		{
			if (t == i)
			{
				return false;
			}
		}
		targets.add(t);
		out_degree++;
		return true;
	}
	
	public boolean AddSource(int s)
	{
		for (int i : sources)
		{
			if (s == i)
			{
				return false;
			}
		}
		sources.add(s);
		in_degree++;
		return true;
	}
	
	public void ProcessNeighbors()
	{
		neighbors = targets;
		for (int sid : sources)
		{
			if (targets.contains(sid))
			{
				bidirected_neighbors.add(sid);
			}
			else
			{
				neighbors.add(sid);
			}
		}
	}
	
	public boolean IsTarget(int id)
	{
		return targets.contains(id);
	}
	
	public boolean IsSource(int id)
	{
		return sources.contains(id);
	}
	
	public boolean IsNeighbor(int id)
	{
		return neighbors.contains(id);
	}
	
	public boolean IsBi(int id)
	{
		return bidirected_neighbors.contains(id);
	}
	
}
