package algorithms.graphIsomorphism;

import modules.Partition;
import util.Edge;
import util.Vertex;
import algorithms.GAS_interface;
import algorithms.GatheredData;
import algorithms.VertexData;
import algorithms.graphIsomorphism.giBasic.StepIndicator;
import algorithms.graphIsomorphism.giBasic.TopologicalRelation;

public class GAS_GraphIsomorphism implements GAS_interface {
	public Pattern pattern;
	public boolean labeled;
	private Partition p;
	
	public GAS_GraphIsomorphism(Partition p)
	{
		pattern = new Pattern("/home/licn/Desktop/test/pattern.txt", false);
		labeled = false;
		this.p = p;
	}
	
	public GAS_GraphIsomorphism(String patternFilePath, boolean labeled, Partition p)
	{
		pattern = new Pattern(patternFilePath, labeled);
		this.labeled = labeled;
		this.p = p;
	}

	@Override
	public VertexData getVertexData(Integer v_id, VertexData oldVertexData, String vertexFile) {
		// TODO Auto-generated method stub
		VertexDataGI data = new VertexDataGI("", pattern);
	/*	VertexDataGI data;
		if(oldVertexData != null)
		{
			data = (VertexDataGI) oldVertexData;
			data.local_pattern = new Pattern(pattern);
			data.matched_subgraph.clear();
			data.messages_in_vertex.messages.clear();
			
			if(data.local_pattern.m_pattern.size()==0)
			{
				System.out.println("Error! Given Pattern does not have any vertex!");
			}
			else if(data.local_pattern.m_pattern.size()==1)
			{
				PatternVertex pv = data.local_pattern.m_pattern.get(0);
				if (SemanticCompare(data,pv))
				{
					MatchingMessage m = new MatchingMessage(data.local_pattern.m_pattern.size());
					m.matched_id.set(pv.id, v_id);
					m.source_pos = pv.id;
					
					data.matched_subgraph.add(m.matched_id);
					
					System.out.println("A subgraph is found! " + m.matched_id);
				}		
			}
			else
			{					
				PatternVertex pv = data.local_pattern.m_pattern.get(0);
				//for (PatternVertex pv : newData.local_pattern.m_pattern)
				{
					if (SemanticCompare(data, pv) && 
							data.sources.size()>= pv.in_degree && data.targets.size()>= pv.out_degree)
					{
						
						MatchingMessage m = new MatchingMessage(data.local_pattern.m_pattern.size());
						m.matched_id.set(pv.id, v_id);
						m.source_pos = pv.id;
						m.forwarding_trace.add(v_id);
						data.messages_in_vertex.messages.add(m);
					}
				}
			}			
		}
		else
		{
			data = new VertexDataGI("", pattern);
		}
			*/
		return data;
	}
	
	@Override
	public int gather_edges() {
		// TODO Auto-generated method stub
		return ALL_EDGES;
	}

	@Override
	public GatheredData gather(Vertex myself, Vertex neighbor, Edge e) {
		// TODO Auto-generated method stub
		GatheredDataGI gathered = new GatheredDataGI();
		MessagesInVertex m_in_v = new MessagesInVertex(((VertexDataGI)neighbor.data()).messages_in_vertex);
		
		//System.out.println(m_in_v.messages.size() + " messages from neighbor " + neighbor.id());
		m_in_v.source = neighbor.id();
		if(e.u == myself.id())
		{
			m_in_v.tr = TopologicalRelation.trTarget;
		}
		else
		{
			m_in_v.tr = TopologicalRelation.trSource;
		}
		
		if(((VertexDataGI) myself.data()).si == StepIndicator.FirstStep || m_in_v.messages.size() != 0)
		{
			gathered.gathered_messages.add(m_in_v);
		}
		
		return gathered;
	}

	@Override
	public GatheredData sum(GatheredData a, GatheredData b) {
		// TODO Auto-generated method stub
		GatheredDataGI gathered = new GatheredDataGI();
		gathered.gathered_messages.addAll(((GatheredDataGI)b).gathered_messages);
				
		for(MessagesInVertex miva: ((GatheredDataGI) a).gathered_messages)
		{
			for(MessagesInVertex mivb: ((GatheredDataGI) b).gathered_messages)
			{
				if(miva.source == mivb.source)
				{
					miva.tr = TopologicalRelation.trBidirection;
					gathered.gathered_messages.remove(mivb);
				}
			}			
		}

		gathered.gathered_messages.addAll(((GatheredDataGI)a).gathered_messages);				
		return gathered;
	}

	@Override
	public void applyScatter(Vertex v, GatheredData sum) {
		// TODO Auto-generated method stub
		VertexDataGI newData = (VertexDataGI) v.data();

		switch(newData.si)
		{	
			/*case GetReady:
			{
				//System.out.println(newData.local_pattern.serialize());
				
				newData.messages_in_vertex.source = v.id();
				newData.si = StepIndicator.FirstStep;
				Partition.signalAllNeighbors(v);
				break;
			}*/
			case FirstStep:
			{
				for(MessagesInVertex m_in_v : ((GatheredDataGI)sum).gathered_messages)
				{
					switch(m_in_v.tr)
					{
						case trTarget:
						{
							newData.targets.add(m_in_v.source);
							newData.neighbors.add(m_in_v.source);
							break;
						}						
						case trSource:
						{
							newData.sources.add(m_in_v.source);
							newData.neighbors.add(m_in_v.source);
							break;
						}						
						case trBidirection: 
						{
							newData.bidirected_neighbors.add(m_in_v.source);
							newData.targets.add(m_in_v.source);
							newData.sources.add(m_in_v.source);
							newData.neighbors.add(m_in_v.source);
							break;
						}
						case trUnknown: 
						case trUnconnected: 
						default:
							break;
					}
				}
				
//				System.out.println("Targets of " + v.id() + " is " + newData.targets);
//				System.out.println("Sources of " + v.id() + " is " + newData.sources);
//				System.out.println("Neigbors of " + v.id() + " is " + newData.neighbors);
//				System.out.println("Bi of " + v.id() + " is " + newData.bidirected_neighbors);
				
				//define label based on in-degree
				if(labeled)
				{
					if(newData.sources.size() > giBasic.threshold_celebrity)
					{
						newData.label = "Celebrity";
					}
					else if(newData.sources.size() > giBasic.threshold_active)
					{
						newData.label = "ActiveUser";
					}
					else if(newData.sources.size() > giBasic.threshold_normal)
					{
						newData.label = "NormalUser";
					}
					else
					{
						newData.label = "InactiveUser";
					}
//					System.out.println(v.id() + " is " + newData.label);
				}
				
				if(newData.local_pattern.m_pattern.size()==0)
				{
					System.out.println("Error! Given Pattern does not have any vertex!");
				}
				else if(newData.local_pattern.m_pattern.size()==1)
				{
					PatternVertex pv = newData.local_pattern.m_pattern.get(0);
					if (SemanticCompare(newData,pv))
					{
						MatchingMessage m = new MatchingMessage(newData.local_pattern.m_pattern.size());
						m.matched_id.set(pv.id, v.id());
						m.source_pos = pv.id;						
//						newData.matched_subgraph.add(m.matched_id);						
//						System.out.println("A subgraph is found in " + v.id() + ": " + m.matched_id);
					}		
				}
				else
				{
					
				//#ifdef INITIAL_OPTIMIZATION
					PatternVertex pv = newData.local_pattern.m_pattern.get(0);
				//#else 
					//for (PatternVertex pv : newData.local_pattern.m_pattern)
				//#endif
					{
						if (SemanticCompare(newData,pv) && 
						newData.sources.size()>= pv.in_degree && newData.targets.size()>= pv.out_degree)
						{
							
							MatchingMessage m = new MatchingMessage(newData.local_pattern.m_pattern.size());
							m.matched_id.set(pv.id, v.id());
							m.source_pos = pv.id;
							m.forwarding_trace.add(v.id());
							newData.messages_in_vertex.messages.add(m);
						}
					}
				}
				
				/*System.out.println(v.id() + " has " + newData.messages_in_vertex.messages.size() + " messages.");
				for(MatchingMessage m: newData.messages_in_vertex.messages)
				{
					System.out.println("New Message in " + v.id() + ": " + m.matched_id);
				}*/
				
				newData.si = StepIndicator.SecondStep;
				//+replicas_cost
				if(newData.messages_in_vertex.messages.size()!= 0)
				{
					v.signalInNeighbors(true);
					v.signalOutNeighbors(true);
				}				
				break;
			}
			case SecondStep:
			{
				newData.messages_in_vertex.messages.clear();
				for(MessagesInVertex m_in_v : ((GatheredDataGI)sum).gathered_messages)
				{
					//+replicas_cost
					//System.out.println("Message from vertex " + m_in_v.source);
					for (MatchingMessage m : m_in_v.messages)
					{
						//+replicas_cost
						//System.out.println("Message: " + m.matched_id);
						
						//If the current gv is already in the message
						if(m.matched_id.contains(v.id()))	
						{
							if(!m.forwarding_trace.contains(v.id()))
							{
								//Forward the message to gv's neighours except the source of the message.
								MatchingMessage new_m = new MatchingMessage(m);
								int found_pos = m.matched_id.indexOf(v.id());
								new_m.source_pos = found_pos;
								new_m.forwarding_trace.add(v.id());
								newData.messages_in_vertex.messages.add(new_m);
							}
							
						}
						else
						{
							//If there is any possible vertex in the pattern, which current gv can match.
							//If found, then update the message and send to all neighbors.
							for (int pv_id : newData.local_pattern.m_pattern.get(m.source_pos).neighbors)
							{
								if ( SemanticCompare(newData,newData.local_pattern.m_pattern.get(pv_id)) && (m.matched_id.get(pv_id) == -1) //same label and still available//-1
									&& newData.sources.size()>= newData.local_pattern.m_pattern.get(pv_id).in_degree && newData.targets.size()>= newData.local_pattern.m_pattern.get(pv_id).out_degree)//comparison of in/out degree
								{
									boolean satisify_pre_matches = true;
									for (int matched_pos = 0; matched_pos < m.matched_id.size(); matched_pos++)
									{
										if (m.matched_id.get(matched_pos) != -1)
										{
											if (
												(newData.local_pattern.m_pattern.get(pv_id).IsBi(matched_pos) && newData.IsBi(m.matched_id.get(matched_pos))) ||
												(newData.local_pattern.m_pattern.get(pv_id).IsTarget(matched_pos) && !newData.local_pattern.m_pattern.get(matched_pos).IsBi(pv_id) && newData.IsTarget(m.matched_id.get(matched_pos))) ||
												(newData.local_pattern.m_pattern.get(pv_id).IsSource(matched_pos) && !newData.local_pattern.m_pattern.get(matched_pos).IsBi(pv_id) && newData.IsSource(m.matched_id.get(matched_pos))) ||
												!newData.local_pattern.m_pattern.get(pv_id).IsNeighbor(matched_pos) 
												)//compare topological relation
											{
												//Do nothing
											}
											else
											{
												satisify_pre_matches = false;
												break;
											}
										}
									}
									
									
									if (satisify_pre_matches)
									{
										MatchingMessage new_m = new MatchingMessage(m);
										new_m.matched_id.set(pv_id, v.id());
										new_m.source_pos = pv_id;
										new_m.forwarding_trace.clear();
										new_m.forwarding_trace.add(v.id());

										if(new_m.matched_id.contains(-1))
										{
											newData.messages_in_vertex.messages.add(new_m);
										}
										else//A subgraph is found!
										{
											if (!newData.IsDuplicated(new_m.matched_id))
											{
//												newData.matched_subgraph.add(new_m.matched_id);
//												System.out.println("A subgraph is found in " + v.id() + ": " + new_m.matched_id);
											}								
										}
									}
								}
							}
						}
					}
				}
				/*System.out.println(v.id() + " has " + newData.messages_in_vertex.messages.size() + " messages.");
				for(MatchingMessage m: newData.messages_in_vertex.messages)
				{
					System.out.println("New Message in " + v.id() + ": " + m.matched_id);
				}*/
				
				//+replicas_cost
				if(newData.messages_in_vertex.messages.size()!= 0)
				{
					v.signalInNeighbors(true);
					v.signalOutNeighbors(true);
				}
				break;
			}
			default:
				break;
		}
	}

	public boolean SemanticCompare(VertexDataGI gv, PatternVertex pv)
	{
		if(labeled) 
			return gv.label.equals(pv.label);
		else
			return true;
	}

	@Override
	public VertexData getEmptyVertexData() {
		return new VertexDataGI();
	}

	@Override
	public GatheredData getEmptyGatheredData() {
		return new GatheredDataGI();
	}

}
