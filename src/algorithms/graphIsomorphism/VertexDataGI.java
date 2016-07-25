package algorithms.graphIsomorphism;

import java.util.ArrayList;
import java.util.List;
import algorithms.VertexData;
import algorithms.graphIsomorphism.giBasic.StepIndicator;

public class VertexDataGI implements VertexData {
	public String label;
	public Pattern local_pattern;
	public StepIndicator si;
	public MessagesInVertex messages_in_vertex = new MessagesInVertex();
	public List<List<Integer>> matched_subgraph = new ArrayList<List<Integer>>();
	public List<Integer> targets = new ArrayList<Integer>();
	public List<Integer> sources = new ArrayList<Integer>();
	public List<Integer> neighbors = new ArrayList<Integer>();
	public List<Integer> bidirected_neighbors = new ArrayList<Integer>();
	
	public VertexDataGI()
	{
		label = "";
		//local_pattern = new Pattern();
		//local_pattern.m_pattern.add(new PatternVertex());
//		si = StepIndicator.GetReady;	
		si = StepIndicator.FirstStep;
	}
	
	public VertexDataGI(String vertexlabel)
	{
		label = vertexlabel;
		//local_pattern = new Pattern();
		//local_pattern.m_pattern.add(new PatternVertex());
//		si = StepIndicator.GetReady;	
		si = StepIndicator.FirstStep;
	}
	
	public VertexDataGI(String vertexlabel, Pattern p)
	{
		label = vertexlabel;
		local_pattern = new Pattern(p);
//		si = StepIndicator.GetReady;	
		si = StepIndicator.FirstStep;
	}
	
	@Override
	public int byteSize() {
		// TODO: size
		return 100;
	}
	
	public boolean IsDuplicated(List<Integer> new_matched)
	{
		int message_length = new_matched.size();
		for (List<Integer> matched : matched_subgraph)
		{
			boolean duplicated = true;
			for (int i = 0; i < message_length; i++)
			{
				if (matched.get(i) != new_matched.get(i))
				{
					duplicated = false;
					break;
				}
			}
			if (duplicated)
			{
				return	true;
			}
		}
		return false;	
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
