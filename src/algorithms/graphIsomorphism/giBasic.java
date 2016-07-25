package algorithms.graphIsomorphism;

public class giBasic {
	public static enum StepIndicator {FirstStep, SecondStep};
	public static enum TopologicalRelation {trUnknown, trUnconnected, trTarget, trSource, trBidirection};
	
	public static int threshold_celebrity = 100;
	public static int threshold_active = 10;
	public static int threshold_normal = 5;
	
//	public static int threshold_celebrity = 3200;
//	public static int threshold_active = 1600;
//	public static int threshold_normal = 200;
	
//
//	public static int threshold_celebrity = 2000;
//	public static int threshold_active = 500;
//	public static int threshold_normal = 100;
}