//package model;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//import general.Methods;
//import general.Topology;
//
//public class ModelCAL {
//
//	public Throughput throughput;
//	public Latency latency;
//	public long thr;
//	public double lat;
//	
//	
//	public ModelCAL(Topology t, int inputrate){
//		this.throughput = new Throughput(t.getTname(), t.getCompo(),inputrate);
//		this.latency = new Latency(t.getTname(),new ArrayList<Double>(), new ArrayList<Double>(),0); 
//		thr = 0;
//		lat = 0.0;
//	}
//
//	
//    public void updateLatency(ArrayList<Double> arr, ArrayList<Double> serv, int numserver){
//    	
//    	latency.updateData(arr, serv, numserver);
//    }
//	
//    public void updatethroughput(HashMap<String, ArrayList<Double>> cp, HashMap<String, ArrayList<Double>> ce,HashMap<String, Long> lp, HashMap<String, Long> lc){
//    	throughput.updateData(cp, ce, lp, lc);
//    }
//	public void performanceMetric(){
////		thr = throughput.totalThroughput();
//		lat = latency.waittimeEstimating();
//	}
//	
//	
//	public static void main(String[] args) {
//		ArrayList<String> names = new ArrayList<String>();
//		names.add("a");
//		names.add("b");
//		names.add("c");
//		names.add("d");
//		names.add("e");
//		names.add("f");
//		ArrayList<Integer> threads = new ArrayList<Integer>();
//		threads.add(1);
//		threads.add(2);
//		threads.add(2);
//		threads.add(2);
//		threads.add(2);
//		threads.add(2);
//		ArrayList<Double> prob = new ArrayList<Double>();
//		prob.add(0.2);
//		prob.add(0.8);
//		ArrayList<Double> prob2 = new ArrayList<Double>();
//		prob2.add(0.5);
//		prob2.add(0.5);
//		ArrayList<Double> prob1 = new ArrayList<Double>();
//		prob1.add(1.0);
//		ArrayList<Double> exe1 = new ArrayList<Double>();
//		exe1.add(1.0);
//		ArrayList<Double> exe = new ArrayList<Double>();
//		exe.add(1.0);
//		exe.add(1.0);
//		
//		ArrayList<Double> arr = new ArrayList<Double>();
////		arr.add(197.67);
////		arr.add(202.95);
////		arr.add(202.95);
//		ArrayList<Double> serv = new ArrayList<Double>();
////		serv.add(91.25);
////		serv.add(91.41);
////		serv.add(91.41);
//		HashMap<String, ArrayList<Double>> cp = new HashMap<String, ArrayList<Double>>();
////		cp.put("a", prob1);
////		cp.put("b", prob);
////		cp.put("c", prob2);
////		cp.put("d", prob1);
//		
//		HashMap<String, ArrayList<Double>> ce = new HashMap<String, ArrayList<Double>>();
////		ce.put("a", exe1);
////		ce.put("b", exe);
////		ce.put("c", exe);
////		ce.put("d", exe1);
//	
//		HashMap<String, Long> lp = new HashMap<String, Long>();
////		lp.put("a", (long) 80);
////		lp.put("b", (long) 80);
////		lp.put("c", (long) 80);
////		lp.put("d", (long) 80);
//		
//		HashMap<String, Long> lc = new HashMap<String, Long>();
////		lc.put("a", (long) 80);
////		lc.put("b", (long) 80);
////		lc.put("c", (long) 80);
////		lc.put("d", (long) 80);
//		
////		ModelTopology t = new ModelTopology("line_topology", names,"line", threads);
////		ModelCAL mc = new ModelCAL(t,2);
////		String name = "line-6-1479438063";
////		arr = Methods.getRecords(name, true);
////		serv = Methods.getRecords(name, false);
////		mc.updateLatency(arr, serv, 4);
////		mc.performanceMetric();
////		System.out.println("latency "+ mc.lat);
////		System.out.println("trhoguhput "+mc.thr);
////		mc.initToplogy(tt);
//	}
//	
//}
