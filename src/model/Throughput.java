package model;

import java.util.ArrayList;
import java.util.HashMap;

import com.google.common.collect.Multiset.Entry;

import general.Component;

public class Throughput {
//	public Topology t;
//	public ArrayList<String> name;
	public String shape;
	public ArrayList<Integer> througput;
	//structured components in layers
	public ArrayList<ArrayList<String>> compostruc;
	//collection of all components
	public HashMap<String,Component> compo;
//	public HashMap<String,Component> list;
	public double inputrate;
	public int layer;
	public HashMap<String, ArrayList<Double>> compoprob;
	public HashMap<String,ArrayList<Double>> compoexe;
//	public HashMap<String,Long> compolastproc;
//	public HashMap<String,Long> compolasttran;
	public HashMap<String, ArrayList<String>> compoports;
	public String tid;
	public Long output; 

	public Throughput(String tname, ArrayList<ArrayList<String>> compostruc, HashMap<String,Component> compo){
		this.compo = compo;
		this.tid = tname;
		this.compostruc = compostruc;
//		this.list = addingcom(components);
//		this.inputrate = inputrate;
		this.layer = compostruc.size();
		this.througput = new ArrayList<Integer>();
//		this.compoprob = new HashMap<String, HashMap<String,Double>>();
		this.compoexe = new HashMap<String, ArrayList<Double>>();
//		this.compolastproc = new HashMap<String, Long>();
//		this.compolasttran = new HashMap<String, Long>();
		this.compoports = new HashMap<String, ArrayList<String>>();
		this.output = (long) 0;
//		this.compoprocess = new HashMap<String, ArrayList<Long>>();
	}
	
	//change the components that arranged by layer to a list for update data purpose
//	public HashMap<String, Component> addingcom(ArrayList<ArrayList<Integer>> c){
//		HashMap<String, Component> list = new HashMap<String, Component>();
//		for(int i = 0 ; i<c.size(); i++){
//			for (int j = 0; j<c.get(i).size();j++){
//			  list.put(c.get(i).get(j).cid, c.get(i).get(j));
//			}
//		}
//		return list;
//	}
	
	public double getInputrate() {
		return inputrate;
	}

	public void setInputrate(double inputrate) {
		this.inputrate = inputrate;
	}

	public long getOutput(){
		long temp = 0;
		for(String cid : compostruc.get(layer-1)){
			temp += Long.valueOf(compo.get(cid).getValuealltime().get("acked"));
		}
		return temp;
	}
	// calculate the total throughput, based on latest observation values
	// every time calculate the throughput, threads and values of each component will be update first
	// adding the latency 
	public long totalThroughput(){
		long l = 0;
		double processed;
		double input = inputrate;
		System.out.println("input is "+input);
		long output = 0;
		
		Component c;
		for(int i =1; i<layer; i++){
			int temp = compostruc.get(i).size();
			for(int j = 0; j<compostruc.get(i).size(); j++){
				String value = compostruc.get(i).get(j);
				c = compo.get(value);
				String s = c.cid;
//				c.updatethreads(c.prob, c.executime);
//				c.updateData(c.lasttrans, c.lastproc);
				
				//suppose that the arrival rate to each downstream is equal
				
				processed = c.totalProcessed(input/temp);
//				c.updateP_T(compolasttran.get(s), compolastproc.get(s));
//				output += c.procTotran();
				long outputtemp = c.procTotran(compo.get(s).lastproc, compo.get(s).lasttrans);
				
//				latency += c.waittimeEstimating();
				output += outputtemp;
//				System.out.println("final result plus "+processed);
				l += processed;
			}
//				System.out.println("output at "+i+" layer is "+output);
				input = output;
				
				output = 0;
//				System.out.println("input at layer "+(i+1)+ " is "+input);
		}
//		System.out.println("totalthorughput final result is "+l);
		return l;
	}
	
	public double throughputRatio(){
		double input = 0;
		double output = 0;	
		Component c;
		System.out.println("size of compostruc "+compostruc.size());
		for(int i  = 0; i<compostruc.get(0).size(); i++){
			String value = compostruc.get(0).get(i);
			c = compo.get(value);
			String s = c.cid;
			input += compo.get(s).getLastemit();
			output += compo.get(s).getLastack();
		}
		System.out.println("inside thorughput ratio, input is "+input+ " , outoput is "+output);
		if(input == 0)
			return 0;
		else if(input<output)
		    return output*1.0/input;
		else
			return 1;
	}
	
	public long tThroughput(){
		long l = 0;
		double processed;
		double input = inputrate;
		long output = 0;
		long result = 0;
//		double latency = 0;
		Component c;
//		for(int i =0; i<layer; i++){
		for(int i =1; i<layer; i++){
			result = 0;
			int temp = compostruc.get(i).size();
			for(int j = 0; j<compostruc.get(i).size(); j++){
				String value = compostruc.get(i).get(j);
				c = compo.get(value);
				String s = c.cid;
//				c.updatethreads(c.prob, c.executime);
//				c.updateData(c.lasttrans, c.lastproc);
				
				//suppose that the arrival rate to each downstream is equal
				
				processed = c.totalProcessed(input/temp);
//				c.updateP_T(compolasttran.get(s), compolastproc.get(s));
//				output += c.procTotran();
				long outputtemp = c.procTotran(compo.get(s).lastproc, compo.get(s).lasttrans);
				
//				latency += c.waittimeEstimating();
				output += outputtemp;
//				System.out.println("final result plus "+processed);
//				l += processed;
			}
//				System.out.println("output at "+i+" layer is "+output);
				result = output;
//				
//				output = 0;
//				System.out.println("input at layer "+(i+1)+ " is "+input);
		}
//		System.out.println("totalthorughput final result is "+l);
		return result;
	}
	//update these observation data by routine function
	public void updateProb(HashMap<String, ArrayList<Double>> cp){
		compoprob = cp;
//		compoexe = ce;
		
	}
	public void updateExeu(HashMap<String, ArrayList<Double>> ce){
		compoexe = ce;
	}

//	public void updatelastProc(HashMap<String, Long> lp){
//		compolastproc = lp;
//	}
//	public void updatelastTran(HashMap<String, Long> lc){
//		compolasttran = lc;
//	}
//	
	
//	//update the prob and exec of each component
//	public void updateComponent(boolean first){
//		for(String s : list.keySet()){
//			Component c = list.get(s);
//			if(first == true)
//				c.(compolasttran.get(s), compolastproc.get(s));
//			c.updatethreads(compoprob.get(s), compoexe.get(s));
//			
//		}
		
//	}
	
//	public void updateData(HashMap<String, ArrayList<Double>> cp, HashMap<String, ArrayList<Double>> ce,HashMap<String, Long> lp, HashMap<String, Long> lc){
//		updateProb(cp);
//		updateExeu(ce);
//		updatelastProc(lp);
//		updatelastTran(lc);
//	}
//	
//	public static void main(String[] args) {
//		
//		ArrayList<String> names = new ArrayList<String>();
//		names.add("a");
//		names.add("b");
//		names.add("c");
//		names.add("d");
//		ArrayList<Integer> threads = new ArrayList<Integer>();
//		threads.add(1);
//		threads.add(2);
//		threads.add(2);
//		threads.add(1);
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
//	//	Throughput tt = new Throughput(names,"star", threads, 5);
////		Topology t = tt.t;
//		
//		HashMap<String, ArrayList<Double>> cp = new HashMap<String,ArrayList<Double>>();
//		cp.put("a", prob1);
//		cp.put("b",prob);
//		cp.put("c", prob2);
//		cp.put("d", prob1);
//		
//		HashMap<String, ArrayList<Double>> ce = new HashMap<String,ArrayList<Double>>();
//		ce.put("a", exe1);
//		ce.put("b", exe);
//		ce.put("c", exe);
//		ce.put("d", exe1);
//		
//		HashMap<String, Long> lp = new HashMap<String, Long>();
//		lp.put("a", (long)80);
//		lp.put("b", (long)20);
//		lp.put("c", (long)20);
//		lp.put("d", (long)20);
//		HashMap<String, Long> lc = new HashMap<String, Long>();
//		lc.put("a", (long)80);
//		lc.put("b", (long)20);
//		lc.put("c", (long)20);
//		lc.put("d", (long)20);
		
//		tt.updateData(cp, ce,lp,lc);
//		tt.updateComponent(true);
		
//		Component sec = t.components.get(1).get(0);
//		sec.setProb(prob);
//		sec.setExecutime(exe);
//		sec.setLastproc(80);
//		sec.setLasttrans(40);
//		
//		Component thi = t.components.get(2).get(0);
//		thi.setProb(prob);
//		thi.setExecutime(exe);
//		thi.setLastproc(80);
//		thi.setLasttrans(80);
		
//		Component fou = t.components.get(3).get(0);
//		fou.setProb(prob1);
//		fou.setExecutime(exe1);
//		fou.setLastproc(80);
//		fou.setLasttrans(80);
//		System.out.print("total throughput is "+tt.totalThroughput());
		
//		ArrayList<String> names = new ArrayList<String>();
//		names.add("a");
//		names.add("b");
//		names.add("c");
//		names.add("d");
//		names.add("e");
//		names.add("f");
//		ArrayList<Integer> threads = new ArrayList<Integer>();
//		threads.add(1);
//		threads.add(1);
//		threads.add(1);
//		threads.add(1);
//		threads.add(1);
//		threads.add(1);
//		Throughput tp = new Throughput(names, "diamond", threads);
//		System.out.println(tp.layer);
//		System.out.println(t);
//		Throughput t = new Throughput(names,"diamond");
//		System.out.println(t.struc);
//		System.out.println(t.layer);
//	}
}
