package algorithms.cellularAutomaton;

import java.util.ArrayList;
import java.util.List;

import algorithms.VertexData;

public class VertexDataCA implements VertexData {
	
	public VertexDataCA() {
		this.queueLeft = new ArrayList<Agent>();
		this.queueRight = new ArrayList<Agent>();
		this.queueUp = new ArrayList<Agent>();
		this.queueDown = new ArrayList<Agent>();
		this.queueLeftUp = new ArrayList<Agent>();
		this.queueLeftDown = new ArrayList<Agent>();
		this.queueRightUp = new ArrayList<Agent>();
		this.queueRightDown = new ArrayList<Agent>();
		
		this.agents = new ArrayList<Agent>();
	}

	// Simulation of agents
	public List<Agent> agents;
	
	// Out queue for each neighbor
	public List<Agent> queueUp;
	public List<Agent> queueDown;
	public List<Agent> queueLeft;
	public List<Agent> queueRight;
	
	public List<Agent> queueLeftUp;
	public List<Agent> queueLeftDown;
	public List<Agent> queueRightUp;
	public List<Agent> queueRightDown;
	
	// responsibility area defined by x- and y-range
	public double x_min;
	public double x_max;
	public double y_min;
	public double y_max;
	
	
//	@Override
//	public String toString() {
//		return serialize();
//	}
//
//	public String serialize() {
//		String s = "";
//		s += Agent.serializeList(agents) + "!";
//		
//		
//		
//		s += Agent.serializeList(queueUp) + "!";
//		s += Agent.serializeList(queueDown) + "!";
//		s += Agent.serializeList(queueLeft) + "!";
//		s += Agent.serializeList(queueRight) + "!";
//		
//		s += Agent.serializeList(queueLeftUp) + "!";
//		s += Agent.serializeList(queueLeftDown) + "!";
//		s += Agent.serializeList(queueRightUp) + "!";
//		s += Agent.serializeList(queueRightDown) + "!";
//		
//		s += x_min + "!";
//		s += x_max + "!";
//		s += y_min + "!";
//		s += y_max + "!";
//		
//		return s;
//	}
//	
//	public void deserialize(String string) {
//		String[] s = string.split("!");
//		
//		this.agents = Agent.deserializeList(s[0]);
//		
//		this.x_min = Double.valueOf(s[9]);
//		this.x_max = Double.valueOf(s[10]);
//		this.y_min = Double.valueOf(s[11]);
//		this.y_max = Double.valueOf(s[12]);
//		
//		this.queueUp = Agent.deserializeList(s[1]);
//		this.queueDown = Agent.deserializeList(s[2]);
//		this.queueLeft = Agent.deserializeList(s[3]);
//		this.queueRight = Agent.deserializeList(s[4]);
//		
//		this.queueLeftUp = Agent.deserializeList(s[5]);
//		this.queueLeftDown = Agent.deserializeList(s[6]);
//		this.queueRightUp = Agent.deserializeList(s[7]);
//		this.queueRightDown = Agent.deserializeList(s[8]);
//	}
	
	public VertexData copy() {
		VertexDataCA d = new VertexDataCA();
		d.x_min = this.x_min;
		d.x_max = this.x_max;
		d.y_min = this.y_min;
		d.y_max = this.y_max;
		
		d.queueUp = new ArrayList<Agent>(queueUp);
		d.queueDown = new ArrayList<Agent>(queueDown);
		d.queueLeft = new ArrayList<Agent>(queueLeft);
		d.queueRight = new ArrayList<Agent>(queueRight);
		
		d.queueLeftUp = new ArrayList<Agent>(queueLeftUp);
		d.queueLeftDown = new ArrayList<Agent>(queueLeftDown);
		d.queueRightUp = new ArrayList<Agent>(queueRightUp);
		d.queueRightDown = new ArrayList<Agent>(queueRightDown);
		
		
		return d;
	}

	@Override
	public int byteSize() {
		int bytes = 0;
		for (Agent a : agents) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueUp) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueDown) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueLeft) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueRight) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueLeftUp) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueLeftDown) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueRightUp) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		for (Agent a : queueRightDown) {
			for (String s : a.movements) {
				bytes += s.getBytes().length;
			}
		}
		return bytes;
	}
}
