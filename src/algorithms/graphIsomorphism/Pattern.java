package algorithms.graphIsomorphism;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Pattern {
	public List<PatternVertex> m_pattern  = new ArrayList<PatternVertex>();
	//public int patternvector_size;
	
	public Pattern()
	{
		
	}
	
	public Pattern(Pattern other)
	{
		this.m_pattern = new ArrayList<PatternVertex>(other.m_pattern);
	}
	
	public Pattern(String file, boolean attributed)
	{
		CreatePattern(file, attributed);
		IntializePattern();
	}
	
	public String serialize() 
	{
		String s = "";
		for(PatternVertex pv : m_pattern)
		{
			s += pv.serialize() + ",";
		}
		return s;
	}
	
	public static Pattern deserialize(String s)
	{
		Pattern p = new Pattern();
		String[] split = s.split(",");
		for(String pvs: split)
		{
			p.m_pattern.add(PatternVertex.deserialize(pvs));
		}
		
		return p;
	}
	
	public void CreatePattern(String file, boolean attributed)
	{
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    for (int i = 0;(line = br.readLine()) != null; i++) 
		    {		       
	    	   int id = i;
	    	   String label = "";
	    	   if(line.isEmpty())
	    	   {
	    		   PatternVertex v = new PatternVertex(id, label);
	    		   m_pattern.add(v);
	    	   }
	    	   else
	    	   {
		    	   String items[] = line.split("\t");
		    	   int other_id;
		    	   int j = 0;
			       if(attributed)
			       {
			    	   label = items[0];
			    	   j++;
			       }
			       PatternVertex v = new PatternVertex(id, label);			       
			       for(;j<items.length;j++)
			       {
			    	   other_id = Integer.parseInt(items[j]);	
			    	   v.AddTarget(other_id);
			       }
			       m_pattern.add(v);	
	    	   }		       	       					
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
	
	public boolean IntializePattern()
	{
		for (PatternVertex v : m_pattern)
		{
			for (int t : v.targets)
			{
				m_pattern.get(t).AddSource(v.id);
			}
		}

		for (PatternVertex v : m_pattern)
		{
			v.ProcessNeighbors();
		}

		return true;
	}
}
