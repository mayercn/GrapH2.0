package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import algorithms.VertexData;


public class Util {

	public static String toString(double[][] matrix) {
		String s = "";
		for (int i=0; i<matrix.length; i++) {
			for (int j=0; j<matrix[i].length; j++) {
				s += matrix[i][j] + " ";
			}
			s += "\n";
		}
		return s;
	}
	
	/**
	 * Returns the smallest integer value in l
	 * @param l
	 * @return
	 */
	public static Integer getMinimal(Collection<Integer> l) {
		Integer smallest = null;
		for (Integer el : l) {
			if (smallest == null || el < smallest) {
				smallest = el;
			}
		}
		return smallest;
	}
	
	/**
	 * Returns true, if all elements in l are true
	 * @param l
	 * @return
	 */
	public static boolean allTrue(Collection<Boolean> l) {
		for (boolean boo : l) {
			if (!boo) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Returns b, if a=c, otherwise a
	 * @param a
	 * @param b
	 * @param c
	 * @return
	 */
	public static int getOther(int a, int b, int c) {
		if (a==c) {
			return b;
		} else {
			return a;
		}
	}
	
	/**
	 * a faster splitting implementation 
	 * @param s string to be split
	 * @return
	 */
	public static String[] fastSplit(String s, int splitSize, char delimitter) {
		
		String[] result = new String[splitSize];
		int a = -1;
		int b = 0;

		for (int i = 0; i < splitSize; i++) {

			while (b < s.length() && s.charAt(b) != delimitter)
				b++;
			result[i] = s.substring(a+1, b);
			a = b;
			b++;		
		}

		return result;
	}
	
//	
//	/**
//	 * Serializes an object to a String
//	 * @param s
//	 * @return
//	 */
//	public static String serialize(Serializable s) {
//		String serializedObject = "";
//
//		 // serialize the object
//		 try {
//		     ByteArrayOutputStream bo = new ByteArrayOutputStream();
//		     ObjectOutputStream so = new ObjectOutputStream(bo);
//		     so.writeObject(s);
//		     so.flush();
//		     serializedObject = bo.toString("ISO-8859-1");
//		     so.close();
//		     bo.close();
//		 } catch (Exception e) {
//		     System.out.println(e);
//		 }
//		 return serializedObject;
//	}
	
//	/**
//	 * Deserializes an object from a string. TODO: test
//	 * @param s
//	 * @return
//	 */
//	public static Serializable deserialize(String s) {
//		Serializable obj = null; 
//		try {
//		     byte b[] = s.getBytes("ISO-8859-1");
//		     ByteArrayInputStream bi = new ByteArrayInputStream(b);
//		     ObjectInputStream si = new ObjectInputStream(bi);
//		     obj = (Serializable) si.readObject();
//		     si.close();
//		     bi.close();
//		 } catch (Exception e) {
//		     System.out.println(e);
//		 }
//		 return obj;
//	}
	
//	/**
//	 * Returns the number of bytes of s in serialized form.
//	 * @param s
//	 * @return
//	 */
//	public static int bytes(Serializable s) {
//		return serialize(s).getBytes().length;
//	}
	
	/**
	 * Returns a string that is a concatenation of the strings in the array starting at the specified position.
	 * @param s
	 * @param startIndex
	 * @return
	 */
	public static String concatenate(String[] s, int startIndex, String fillWith) {
		String res = s[startIndex];
		for (int i=startIndex+1; i<s.length; i++) {
			res += fillWith + s[i];
		}
		return res;
	}
	
	public static String serializeList(List<Integer> l) {
		if (l==null) {
			return "null";
		}
		StringBuilder s = new StringBuilder();
		String separator = "m";
		if (l.size() == 0) {
			return separator;
		}
		for (Integer i : l) {
			s.append(i);
			s.append(separator);
		}
		return s.toString();
	}
	
	public static List<Integer> deserializeList(String s) {
		if (s.equals("null")) {
			return null;
		}
		List<Integer> l = new ArrayList<Integer>();
		if (s.equals(""))
			return l;
		String[] a = s.split("m");
		for (String string : a) {
			l.add(Integer.valueOf(string));
		}
		return l;
	}
	
	public static String serializeIntArray(int[] l) {
		if (l==null) {
			return "null";
		}
		StringBuilder s = new StringBuilder();
		String separator = "m";
		if (l.length == 0) {
			return separator;
		}
		for (int i : l) {
			s.append(i);
			s.append(separator);
		}
		return s.toString();
	}
	
	public static int[] deserializeIntArray(String s) {
		if (s.equals("null")) {
			return null;
		}
		if (s.equals(""))
			return new int[0];
		String[] a = s.split("m");
		int[] l = new int[a.length];
		int i=0;
		for (String string : a) {
			l[i] = Integer.valueOf(string);
			i++;
		}
		return l;
	}
	
	public static String serializeStringList(List<String> l, String separator) {
		if (l==null) {
			return "null";
		} else if (l.size()==0) {
			return "";
		}
		StringBuilder s = new StringBuilder();
		for (int i=0; i<l.size()-1; i++) {
			s.append(l.get(i));
			s.append(separator);
		}
		s.append(l.get(l.size()-1));
		return s.toString();
	}
	
	public static List<String> deserializeStringList(String s, String separator) {
		if (s.equals("null")) {
			return null;
		} else if (s.isEmpty()) {
			return new ArrayList<String>();
		}
		List<String> l = new ArrayList<String>();
		String[] a = s.split(separator);
		for (String string : a) {
			l.add(string);
		}
		return l;
	}

	/**
	 * Copies the list of edges.
	 * @param edges
	 * @return
	 */
	public static List<Edge> copyEdgeList(List<Edge> edges) {
		List<Edge> l = new ArrayList<Edge>();
		for (Edge e : edges) {
			l.add(new Edge(e.u, e.v));
		}
		return l;
	}

	public static List<Integer> copyList(List<Integer> l1) {
		List<Integer> l = new ArrayList<Integer>();
		for (Integer i : l1) {
			l.add(i);
		}
		return l;
	}
	
	public static Set<Integer> copySet(Set<Integer> s1) {
		Set<Integer> s = new HashSet<Integer>();
		for (Integer i : s1) {
			s.add(i);
		}
		return s;
	}
	
	/**
	 * Sorts the list l by descending vertex traffic. The first element has highest traffic.
	 * @param l
	 * @return
	 */
	public static void sortByVertexTraffic(List<Vertex> l) {
		VertexComparator comparator = new VertexComparator();
		Collections.sort(l, comparator);
	}

//	/**
//	 * Serializes the list of vertex data to a String
//	 * @param dataList
//	 * @return
//	 */
//	public static String serializeDataList(List<VertexData> dataList) {
//		if (dataList==null) {
//			return "null";
//		}
//		String s = "";
//		String separator = "€€$";
//		if (dataList.size() == 0) {
//			return separator;
//		}
//		for (VertexData data : dataList) {
//			s += data.serialize() + separator;
//		}
//		return s;
//	}
	
	/**
	 * Returns true, if a new master is selected for vertex newV or the
	 * replica set has changed
	 * @param oldV
	 * @param newV
	 * @return
	 */
	public static boolean vertexHasChanged(Vertex oldV, Vertex newV) {
		if (oldV.master()!=newV.master()) {
			return true;
		}
		if (oldV.replicaSet().size()!=newV.replicaSet().size()) {
			return true;
		}
		for (Integer replica : newV.replicaSet()) {
			if (!oldV.replicaSet().contains(replica)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Flat copy of edge set.
	 * @param hashSet
	 * @return
	 */
	public static HashSet<Edge> copyEdgeSet(HashSet<Edge> hashSet) {
		HashSet<Edge> copySet = new HashSet<Edge>();
		for (Edge e : hashSet) {
			copySet.add(e);
		}
		return copySet;
	}
	
//	private static List<VertexData> deserializeDataList(String s) {
//		if (s.equals("null")) {
//			return null;
//		}
//		List<VertexData> l = new ArrayList<VertexData>();
//		if (s.equals(""))
//			return l;
//		String[] a = s.split("€€$");
//		for (String string : a) {
//			l.add(VertexData.deserialize(string));
//		}
//		return l;
//	}
	
	public static void main(String[] args) {
		long time = System.nanoTime();
		int[] a = new int[1000];
		for (int i=0; i<a.length; i++) {
			a[i] = i + 100000;
		}
		
		for (int i=0; i<1000; i++) {
			Util.serializeIntArray(a);
		}
		System.out.println("Time needed: " + (System.nanoTime() - time));
	}
}
