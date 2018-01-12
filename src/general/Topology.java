package general;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.sun.research.ws.wadl.Method;

public class Topology {
	//host and port for each topology
//	HashMap<String, ArrayList<Long>> tworker;
	ArrayList<Executor> tworker;
	//component name and port number 
	HashMap<String, ArrayList<Executor>> tthread;
	// executor states for each component each thread
	ArrayList<ExecutorStats> exestats;
	String tid;
	public String tname;
	String shape;
	HashMap<String,Component> compo;
	ArrayList<ArrayList<String>> compostruct;
//	Long uptime;
	Long workers;
	int layer;
	Map<String, Long> bolts;
	// latest emit amount
	double systememit;
	// entire latency over the execution
	double systemlatency;
	// latency latency
	double systemlatencylatest;
	String failrate;
	long failed;
	
	HashMap<String, String> cschedule;
	HashMap<String, String> changedschedule;

	//	HashMap<String, Long> systemcoemit;
	HashMap<String, Double>systemcolatency;
	public Topology(String tid, String tname){
		this.tname = tname;
		this.tid = tid;
		this.tworker = new ArrayList<Executor>();
		this.tthread = new HashMap<String, ArrayList<Executor>>();
		this.compo = new HashMap<String,Component>();
		this.compostruct = new ArrayList<ArrayList<String>>();
//		this.uptime = (long) 0;
		this.shape = tname;
		this.layer = 0;
		this.systememit = 0;
		this.systemlatency = 0.0;
//		this.systemcoemit = new HashMap<String,Long>();
		this.systemcolatency = new HashMap<String,Double>();
		this.failrate = "0";
		this.cschedule = new HashMap<String,String>();
		this.changedschedule = new HashMap<String, String>();
	}
	


	public String getFailrate() {
		return failrate;
	}



	public void setFailrate(String failrate) {
		this.failrate = failrate;
	}



	public double getSystememit() {
		return systememit;
	}



	public long getFailed() {
		return failed;
	}



	public void setFailed(long failed) {
		this.failed = failed;
	}


	public void setSystememit(double systememit) {
		this.systememit = Double.valueOf(Methods.formatter.format(systememit));
	}



	public double getSystemlatency() {
		return systemlatency;
	}



	public void setSystemlatency(double systemlatency) {
		this.systemlatency = systemlatency;
	}



//	public HashMap<String, Long> getSystemcoemit() {
//		return systemcoemit;
//	}
//
//
//
//	public void setSystemcoemit(HashMap<String, Long> systemcoemit) {
//		this.systemcoemit = systemcoemit;
//	}



	public double getSystemlatencylatest() {
		return systemlatencylatest;
	}



	public void setSystemlatencylatest(double systemlatencylatest) {
		this.systemlatencylatest = systemlatencylatest;
	}



	public HashMap<String, String> getCschedule() {
		return cschedule;
	}



	public void setCschedule(HashMap<String, String> cschedule) {
		this.cschedule = cschedule;
	}



	public HashMap<String, String> getChangedschedule() {
		return changedschedule;
	}



	public void setChangedschedule(HashMap<String, String> changedschedule) {
		this.changedschedule = changedschedule;
	}



	public HashMap<String, Double> getSystemcolatency() {
		return systemcolatency;
	}



	public void setSystemcolatency(HashMap<String, Double> systemcolatency) {
		this.systemcolatency = systemcolatency;
	}



	public ArrayList<String> getNames(){
		ArrayList<String> result = new ArrayList<String>();
		for(String s: compo.keySet())
			result.add(s);
		for(String s : bolts.keySet()){
			result.add(s);
		}
		return result;
	}
	public void initopology(){
	    ArrayList<String> names = getNames();
		// layered components
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		int count = names.size();
//		int pointindex = 0;
		// the first layer of the components
		ArrayList<String> flayer = new ArrayList<String>();
		//add the spout id to the compo
		flayer.add(names.get(0));
//		Component starting = new Component(names.get(pointindex), tthread.get.get(pointindex), true);
//		flayer.add(starting);
//		com.add(flayer);
		result.add(flayer);
		count--;
		// update shape.equals("diamond") to shape.contains("diamond")
		if(shape.contains("line") && names.size()>1){
			LineT(names, result, count);
		}
		else if(shape.contains("diamond") && names.size()>1){
			if(names.size()<3)
				LineT(names, result, count);
			else
				DiamondT(names,result,count);
		}
		else if(shape.contains("star") && names.size()>1){
			if(names.size()<=3)
				LineT(names, result, count);
			else
				StarT(names,result,count);
				
		}
		setCompostruct(result);
	}
//
	public ArrayList<ArrayList<String>> LineT(ArrayList<String> names, ArrayList<ArrayList<String>> com, int count){
		ArrayList<ArrayList<String>> list = com;
		HashMap<String,Component> update = compo;
		int index = names.size()-count;
		
		Component c;
		while(count>0){
			String n = names.get(index);
			ArrayList<String> temp = new ArrayList<String>();
			if(count!=1)
				c = new Component(n, bolts.get(n));
			else
				c = new Component(n, bolts.get(n),false);
			update.put(c.cid,c);
//			System.out.println("now is "+getCompo().toString());
			temp.add(c.cid);
			list.add(temp);
			index++;
			count--;
		}
		if(index>layer)
			layer = index;
		setCompo(update);
		return list;
	}
	
	
	public ArrayList<ArrayList<String>> DiamondT(ArrayList<String> names, ArrayList<ArrayList<String>> com, int count){
		ArrayList<ArrayList<String>> list = com;
		Component c;
		HashMap<String,Component> update = compo;
		int index = names.size()-count;
		int i = index;
		ArrayList<String> temp = new ArrayList<String>();
		String n = "";
		while(count>1){
			n = names.get(i);
			c = new Component(n, bolts.get(n));
			update.put(c.cid,c);
			temp.add(c.cid);
			i++;
			count--;
		}
		list.add(temp);
		ArrayList<String> lastlayer = new ArrayList<String>();
		n = names.get(names.size()-1);
		c = new Component(n, bolts.get(n),false);
		update.put(c.cid,c);
		lastlayer.add(c.cid);
		
		list.add(lastlayer);
		if(index+2>layer)
			layer = index+2;
		setCompo(update);
		return list;
	}
	
	public ArrayList<ArrayList<String>> StarT(ArrayList<String> names, ArrayList<ArrayList<String>> com, int count){
		ArrayList<ArrayList<String>> list = com;
		int index = names.size()-count;
		HashMap<String,Component> update = compo;
		ArrayList<String> secondlayer = new ArrayList<String>();
		String n = names.get(index);
		Component c = new Component(n, bolts.get(n));
		update.put(c.cid,c);
		secondlayer.add(c.cid);
		list.add(secondlayer);
		index++;
		count--;
		int i = index;
		ArrayList<String> temp = new ArrayList<String>();
		while(count>0){
			n = names.get(i);
			if(count!=1)
				c = new Component(n, bolts.get(n));
			else
				c = new Component(n, bolts.get(n),false);
			update.put(c.cid,c);
			temp.add(c.cid);
			i++;
			count--;
		}
	
		list.add(temp);
		if (index+1>layer)
			layer = index+1;
		setCompo(update);
		return list;
	}
//	
//	
//	
	public void printTopology()
	{
		String s = "";
		for (int i = 0; i<compostruct.size(); i++){
			if(i == 0)
				s += compostruct.get(i).toString()+"\n";
			else if (i == compostruct.size()-1)
				s += compostruct.get(i).toString();
			else 
				s += " the " + i + "'s layers "+ compostruct.get(i).toString()+"\n";
		}
		
		System.out.println(s);
		
	}
//

//	public static void main(String[] args) {
//		ArrayList<String> names = new ArrayList<String>();
//		names.add("a");
//		names.add("b");
//		names.add("c");
////		names.add("d");
////		names.add("e");
////		names.add("f");
//		ArrayList<Integer> threads = new ArrayList<Integer>();
//		threads.add(1);
//		threads.add(2);
//		threads.add(2);
////		threads.add(1);
////		threads.add(1);
////		threads.add(1);
//		Topology t = new Topology(names,"line", threads);
//		Component sec = t.components.get(1).get(0);
//		ArrayList<Double> prob = new ArrayList<Double>();
//		prob.add(0.2);
//		prob.add(0.8);
//		ArrayList<Double> exe = new ArrayList<Double>();
//		exe.add(0.3);
//		exe.add(0.3);
//		sec.updatethreads(prob, exe);
//		Component third = t.components.get(2).get(0);
//		third.updatethreads(prob, exe);
////		System.out.print(sec.totalProcessed((long)100));
//		System.out.print(third.totalProcessed((long)1000));
////		t.printTopology(t.components);
////		System.out.println(t.layer);
////		System.out.println(t);
////		System.out.println(t.components);
//	}
//}
	
	public void setTworker(ArrayList<Executor> tworker) {
		this.tworker = tworker;
	}

	public String getTname() {
		return tname;
	}

	public void setTname(String tname) {
		this.tname = tname;
	}

	public Map<String, Long> getBolts() {
		return bolts;
	}

	public void setBolts(Map<String, Long> bolts) {
		this.bolts = bolts;
	}
	
	public HashMap<String, ArrayList<Executor>> getTthread() {
		return tthread;
	}

	public void setTthread(HashMap<String, ArrayList<Executor>> tthread) {
		this.tthread = tthread;
	}

	public ArrayList<ExecutorStats> getExestats() {
		return exestats;
	}

	public void setExestats(ArrayList<ExecutorStats> exestats) {
		this.exestats = exestats;
	}

	public String getTid() {
		return tid;
	}

	public void setTid(String tid) {
		this.tid = tid;
	}

	public String getShape() {
		return shape;
	}

	public void setShape(String shape) {
		this.shape = shape;
	}

	public HashMap<String,Component> getCompo() {
		return compo;
	}

	public void setCompo(HashMap<String,Component> compo) {
		this.compo = compo;
	}

	public ArrayList<ArrayList<String>> getCompostruct() {
		return compostruct;
	}

	public void setCompostruct(ArrayList<ArrayList<String>> compostruct) {
		this.compostruct = compostruct;
	}

//	public Long getUptime() {
//		return uptime;
//	}
//
//	public void setUptime(Long uptime) {
//		this.uptime = uptime;
//	}

	@Override
	public String toString() {
		return "tid "+tid+" components : "+compo.toString();
	}

	public Long getWorkers() {
		return workers;
	}

	public void setWorkers(Long workers) {
		this.workers = workers;
	}

	public ArrayList<Executor> getTworker() {
		return tworker;
	}
	
	

}

