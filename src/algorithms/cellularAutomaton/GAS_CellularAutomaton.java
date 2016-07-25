package algorithms.cellularAutomaton;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import algorithms.GAS_interface;
import algorithms.GatheredData;
import algorithms.VertexData;
import modules.Partition;
import util.Edge;
import util.Vertex;

public class GAS_CellularAutomaton implements GAS_interface {

	static final double epsilon = 0.0000000000000001;
	public int timeStep;	//s
	private Partition p;
	
	public GAS_CellularAutomaton(Partition p)
	{
		timeStep = 3600;
		this.p = p;
	}
	
	public GAS_CellularAutomaton(int ts, Partition p)
	{
		timeStep = ts;
		this.p = p;
	}
	
	/**
	 * Specifies whether to gather on in-edges, out-edges or all edges
	 * @return
	 */
	public int gather_edges() {
		return GAS_interface.IN_EDGES;
	}
	
	/**
	 * Gathers the vertex data from the neighbor.
	 * For a neighbor cell, the gathered value is the set of agents that
	 * are handed to the gathering cell.
	 * @param neighbor
	 * @return
	 */
	@Override
	public GatheredDataCA gather(Vertex myself, Vertex neighbor, Edge e) {
		GatheredDataCA gathered = new GatheredDataCA();
		String pos = relativePosition(myself, neighbor);
		VertexDataCA data = (VertexDataCA) neighbor.data();
		if (pos.equals("L")) {
			gathered.agents = data.queueLeft;
		} else if (pos.equals("R")) {
			gathered.agents = data.queueRight;
		} else if (pos.equals("U")) {
			gathered.agents = data.queueUp;
		} else if (pos.equals("D")) {
			gathered.agents = data.queueDown;
		}
		
		else if (pos.equals("LU")) {
			gathered.agents = data.queueLeftUp;
		}else if (pos.equals("LD")) {
			gathered.agents = data.queueLeftDown;
		}else if (pos.equals("RU")) {
			gathered.agents = data.queueRightUp;
		}else if (pos.equals("RD")) {
			gathered.agents = data.queueRightDown;
		}
		return gathered;
	}

	/**
	 * Sums two gathered objects together, i.e., performs
	 * a union on the gathered agents.
	 * @param a
	 * @param b
	 * @return
	 */
	@Override
	public GatheredDataCA sum(GatheredData a, GatheredData b) {
		GatheredDataCA gathered = new GatheredDataCA();
		gathered.agents = new ArrayList<Agent>();
		gathered.agents.addAll(((GatheredDataCA)a).agents);
		gathered.agents.addAll(((GatheredDataCA)b).agents);
		return gathered;
	}

	@Override
	public void applyScatter(Vertex v, GatheredData sum) {
		//System.out.println("Superstep: " + Partition.superstep());
		//System.out.println("Gathering vertex " + v + " input sum: " + sum.serialize());
		VertexDataCA newData = (VertexDataCA)v.data();
		newData.queueLeft.clear();
		newData.queueRight.clear();		
		newData.queueUp.clear();
		newData.queueDown.clear();
		
		newData.queueLeftUp.clear();
		newData.queueLeftDown.clear();
		newData.queueRightUp.clear();
		newData.queueRightDown.clear();
		
		for (Agent agent : ((GatheredDataCA)sum).agents) {
			newData.agents.add(agent);
		}
		if (newData.agents.size()>0) {
			//System.out.println("Agents on vertex " + v + ": " + newData.agents.size());
		}
		Iterator<Agent> iter = newData.agents.iterator();
		while (iter.hasNext()) {
			Agent agent = iter.next();
			String pos = pos(agent);
			if(pos.equals(agent.movements.get(agent.movements.size()-1)))
			{
				iter.remove();
				System.out.println("Agent reaches the end.");
			}
			else
			{
				String[] s = pos.split("-");
				double x = Double.valueOf(s[0]);
				double y = Double.valueOf(s[1]);
				if (x<newData.x_min) {	
					if (y>newData.y_max) {
						newData.queueLeftUp.add(agent);
						iter.remove();
						System.out.println("Agent in " + v.id() + " moves left up.");
					} else if (y<newData.y_min) {
						newData.queueLeftDown.add(agent);
						iter.remove();
						System.out.println("Agent in " + v.id() + " moves left down.");
					} else {
						newData.queueLeft.add(agent);
						iter.remove();
						System.out.println("Agent in " + v.id() + " moves left.");
					}
				} else if (x>newData.x_max) {	
					if (y>newData.y_max) {
						newData.queueRightUp.add(agent);
						iter.remove();
						System.out.println("Agent in " + v.id() + " moves right up.");
					} else if (y<newData.y_min) {
						newData.queueRightDown.add(agent);
						iter.remove();
						System.out.println("Agent in " + v.id() + " moves right down.");
					} else{
						newData.queueRight.add(agent);
						iter.remove();
						System.out.println("Agent in " + v.id() + " moves right.");
					}
				} else if (y>newData.y_max) {
					newData.queueUp.add(agent);
					iter.remove();
					System.out.println("Agent in " + v.id() + " moves up.");
				} else if (y<newData.y_min) {
					newData.queueDown.add(agent);
					iter.remove();
					System.out.println("Agent in " + v.id() + " moves down.");
				} 
			}			
		}

		if (newData.agents.size()>0) {
			v.setScheduled(true);
		}
		if (newData.queueLeft.size()>0 || newData.queueRight.size()>0
				|| newData.queueUp.size()>0 || newData.queueDown.size()>0 ) {
			v.signalOutNeighbors(true);
		}
	}
	
	
	/**
	 * Returns the last position of the agent this time step.
	 * In each superstep, he walks timeStep secs, so after k supersteps
	 * he would have walked for k*timeStep seconds.
	 * @param agent
	 * @return
	 */
	public String pos(Agent agent) {
		String[] startPos;
		int t_cur = timeStep * p.superstep();
		while(agent.movements.size() >= 2)
		{
			startPos = agent.movements.get(0).split("-");
			int t1 = Integer.valueOf(startPos[2]);
			double x1 = Double.valueOf(startPos[0]);
			double y1 = Double.valueOf(startPos[1]);
			
			if(t1 >= t_cur)
			{
				System.out.println(x1 + "-" + y1 + "-" + t_cur);
				return x1 + "-" + y1 + "-" + t_cur;					
			}
			else
			{
				String[] endPos = agent.movements.get(1).split("-");
				int t2 = Integer.valueOf(endPos[2]);
				double x2 = Double.valueOf(endPos[0]);
				double y2 = Double.valueOf(endPos[1]);
				if(t2 >= t_cur)
				{
					double v_x = x2-x1;
					double v_y = y2-y1;					
					double x_pos = x1 + v_x*(double)(t_cur-t1)/(double)(t2-t1);
					double y_pos = y1 + v_y*(double)(t_cur-t1)/(double)(t2-t1);
					System.out.println(x_pos + "-" + y_pos + "-" + t_cur);
					return x_pos + "-" + y_pos + "-" + t_cur;
				}
				else
				{
					if(agent.movements.size() == 2)
					{
						System.out.println(agent.movements.get(1));
						return agent.movements.get(1);
					}
					else
					{						
						agent.movements.remove(0);
					}
				}				
			}
		}
		/*
		String[] startPos = agent.movements.get(0).split("-");
		String[] endPos = agent.movements.get(1).split("-");
		double x_0 = Double.valueOf(startPos[0]);
		double y_0 = Double.valueOf(startPos[1]);
		int t_0 = Integer.valueOf(startPos[2]);
		double x_end = Double.valueOf(endPos[0]);
		double y_end = Double.valueOf(endPos[1]);
		int t_end = Integer.valueOf(endPos[2]);
		int t_cur = timeStep * Partition.superstep();
		if(t_cur<= t_0)
		{
			return x_0 + "-" + y_0 + "-" + t_cur;
		}
		else if(t_cur<= t_end)
		{
			double v_x = x_end-x_0;
			double v_y = y_end-y_0;
			
			double x_pos = x_0 + v_x*(double)(t_cur-t_0)/(double)(t_end-t_0);
			double y_pos = y_0 + v_y*(double)(t_cur-t_0)/(double)(t_end-t_0);
			return x_pos + "-" + y_pos + "-" + t_cur;
			
		}
		else
		{
			return agent.movements.get(1);
		}
		*/
		return null;
	}
	
	/**
	 * Returns "L", "R", "U", "D", dependent whether myself is "left", "right", "up" or "down" w.r.t. neighbor
	 * @param myself
	 * @param neighbor
	 */
	public static String relativePosition(Vertex myself, Vertex neighbor) {
		VertexDataCA myselfData = (VertexDataCA) myself.data();
		VertexDataCA neighborData = (VertexDataCA) neighbor.data();
		if (myselfData.x_min+epsilon<neighborData.x_min) {
			if (myselfData.y_max+epsilon<neighborData.y_max) {
				return "LD";
			}
			else if (myselfData.y_min>neighborData.y_min+epsilon) {
				return "LU";
			}
			else
				return "L";
		} else if (myselfData.x_max>neighborData.x_max+epsilon) {
			if (myselfData.y_max+epsilon<neighborData.y_max) {
				return "RD";
			}
			else if (myselfData.y_min>neighborData.y_min+epsilon) {
				return "RU";
			}
			else
				return "R";			
		} else if (myselfData.y_max+epsilon<neighborData.y_max) {
			return "D";
		} else if (myselfData.y_min>neighborData.y_min+epsilon) {
			return "U";
		} else {
			return null;
		}
	}

	/**
	 * Reads the vertex data from a local input file.
	 * Vertex file format:
	 * 							vertex_id	x_min	x_max	y_min	y_max	[serializedAgents]
	 * Serialized agents format:
	 * 							agent1,agent2,...,agentk
	 * Serialized agent format:
	 * 							line1mline2mline3m...mlinel
	 * Serialized line format:
	 * 							word1-word2-...-wordp
	 */
	@Override
	public VertexData getVertexData(Integer v_id, VertexData oldVertexData, String vertexFile) {
		String line = null;
		//One file
		/*try {
			BufferedReader br = new BufferedReader(new FileReader(new File(vertexFile)));
			while((line = br.readLine())!=null) {
				if (line.startsWith(v_id + " ")) {
					break;
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		//One million files
		/*try {
			BufferedReader br = new BufferedReader(new FileReader(new File(vertexFile + v_id)));
			line = br.readLine();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		//One thousand files
		/*int fileIdx = v_id/1000;
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(vertexFile + fileIdx)));
			while((line = br.readLine())!=null) {
				if (line.startsWith(v_id + " ")) {
					break;
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
								
		String[] split = line.split("\\s+");
		double x_min = Double.valueOf(split[1]);
		double x_max = Double.valueOf(split[2]);
		double y_min = Double.valueOf(split[3]);
		double y_max = Double.valueOf(split[4]);
		List<Agent> agents;
		if (split.length>5) {
			agents = Agent.deserializeList(split[5]);
			System.out.println("Vertex " + v_id + " initialized with agents: " + agents.size());
		} else {
			agents = new ArrayList<Agent>();
		}*/
		
		//Given the agent file, load all trajectories
		//int fileIdx = v_id/1000;
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(vertexFile)));
			while((line = br.readLine())!=null) {
				if (line.startsWith(v_id + "")) {
					break;
				}
			}
			br.close();
		} catch (IOException e) {
			System.out.println("Error when reading vertex file!!!!!!!!");
			e.printStackTrace();
		}
		String[] split = line.split("\\s+");
		double x_min = Double.valueOf(split[1]);
		double x_max = Double.valueOf(split[2]);
		double y_min = Double.valueOf(split[3]);
		double y_max = Double.valueOf(split[4]);
		List<Agent> agents = new ArrayList<Agent>();
		if (split.length>5) {
//			String agentFiles[] = split[5].split(",");
//			for(String s : agentFiles)
//			{
//				agents.add(new Agent(s));
//			}
			agents = Agent.deserializeList(split[5]);
			System.out.println("Vertex " + v_id + " initialized with agents: " + agents.size());
		} else {
			agents = new ArrayList<Agent>();
		}
		VertexDataCA d = new VertexDataCA();
		d.agents = agents;
		d.x_max = x_max;
		d.x_min = x_min;
		d.y_max = y_max;
		d.y_min = y_min;
		return d;
	}

	@Override
	public VertexData getEmptyVertexData() {
		return new VertexDataCA();
	}

	@Override
	public GatheredData getEmptyGatheredData() {
		return new GatheredDataCA();
	}
}
