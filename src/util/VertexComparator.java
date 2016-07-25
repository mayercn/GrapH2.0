package util;

import java.util.Comparator;

public class VertexComparator implements Comparator<Vertex> {

	@Override
	public int compare(Vertex v0, Vertex v1) {
		if (v0.trafficPrediction()<v1.trafficPrediction()) {
			return 1;
		} else if (v0.trafficPrediction()==v1.trafficPrediction()) {
			return 0;
		} else {
			return -1;
		}
	}

}
