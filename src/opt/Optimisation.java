package opt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections.set.SynchronizedSortedSet;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;

import edu.umd.cs.findbugs.annotations.Priority;
import general.Component;
import general.Methods;
import general.Topology;

/**
 * running periodically to search for the optimal configuration
 * @author yidwa
 *
 */
public class Optimisation {
	int ns;
	int nm;
	int nl;
	// rate of execution latency from -1 to 0 and rate from 0 to 1
	static double r1 = 2.0/3;
	static double r2 = 0.75;
	ArrayList<Integer> num;
	// index -1 represents small vm, 0 is medium vm and 1 is large vm
	// need to initialise the slot number for each type of vm
	static HashMap<String,Integer> usage;
	
	public Optimisation(){
		this.usage = new HashMap<>();
	}
	
	/**
	 * find the most suitable set of hosts
	 * @param currentindex current type of host
	 * @param est estimated capability of processing amount
	 * @param pre predicted incoming amount
	 * @param latency
	 * @return
	 */
	public static int findType(int currentindex, double est, double pre, double latency){
		int typeindex = 0;
		double lolat= 0.0;
		int loindex = 0;
		double hilat= 0.0;
		int hiindex = 0;
		
		if(currentindex == -1){
			lolat = latency*r1;
			hiindex = 1;
			hilat = r2*lolat;
		}
		else if(currentindex == 0){
			loindex = -1;
			lolat = latency/r1;
			hiindex = 1;
			hilat = latency*r2;
		}
		else{
			hiindex = 0;
			hilat = latency/r2;
			loindex = -1;
			lolat = hilat/r1;
		}
		//set-point trajectory
//		double desire = (pre - est)* Math.pow(Math.exp(1.0), (-1/3.0))+est;
		double desire = pre;
		
		//target at number of tuples that come since thorughput result not that good
//		double desire = pre;
		
		ArrayList<Double> result = new ArrayList<Double>();
		Double temp = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(lolat)));
		result.add(desire-temp);
		temp = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(latency)));
		result.add(desire-temp);
		temp = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(hilat)));
		result.add(desire-temp);
//		System.out.println(result.toString());
		double min = result.get(0);
		int tempindex = 0;
        for (int i = 1 ; i<result.size();i++){
           	if(result.get(i)<min){
           		min = result.get(i);
           		tempindex =i;
           	
           	}
        }
//    	if(tempindex!=currentindex+1){
//   			
//   			if(num.get(0)+num.get(1)+num.get(2)<24)
//   				num.set(currentindex+1, num.get(currentindex+1)+1);
//   			num.set(tempindex, num.get(tempindex)-1);
//   		}
    	
		typeindex =tempindex-1;
//		System.out.println(num.toString());
		return typeindex;
	}


	/**
	 * update the estimated usage according the the current assignment
	 * @param topologies
	 * @param priority
	 */
	public static void udpateUsage(HashMap<String, Topology> topologies, HashMap<String, Integer> priority){
		HashMap<String, Integer> usage = new HashMap<>();
		HashMap<String, String> cscheduler;
		
		usage.put("s1", 0);
		usage.put("s2", 0);
		usage.put("m1", 0);
		usage.put("m2", 0);
		usage.put("l1", 0);
		usage.put("l2", 0);
		
		//udpate the usage status 
		for(String tid: topologies.keySet()){
			cscheduler = topologies.get(tid).getCschedule();
			for(String ctid : cscheduler.keySet()){
				int update = getUsageRatio(cscheduler.get(ctid));
				String type = cscheduler.get(ctid);
				int now = 0;
				if(usage.containsKey(type))
					now = usage.get(type);
					
				usage.put(type,now+update);
			}
		}	
		setUsage(usage);
	}
	
	public static HashMap<String, String> cpuscheduelr(HashMap<String, Topology> topologies, HashMap<String, Integer> priority){
//		System.out.println("enter cpu scheduler");
		HashMap<String, String> temp = new HashMap<>();
		ArrayList<String> needscheduler = new ArrayList<String>();
//		HashMap<String, Integer> priorityofrest = new HashMap<String, Integer>();
//		for(Entry e: priority.entrySet()){
//			priorityofrest.put((String)e.getKey(), (Integer)e.getValue());
//		}

//		System.out.println("prioirtyrest size "+priorityofrest.size());
		HashMap<String, String> current;
		HashMap<String, String> changed;
		udpateUsage(topologies, priority);
//		System.out.println("cpuscheduler 1");
		for(Topology t: topologies.values()){
//			needscheduler.add(t.tname);
			needscheduler.add(t.getTid());
		}
		

// !    cancel the priority for the timing being 
		
		while(needscheduler.size() >0){
//			 String top = findTopt(priorityofrest);
//			 System.out.println("find top "+top);
//			 priorityofrest.remove(top);
//			
//			 Topology t = topologies.get(top);
			 Topology t = topologies.get(needscheduler.get(0));
			 current = t.getCschedule();
			 changed = t.getChangedschedule();
			 changed = tschedule(t.getCompo(),t.tname, current, changed);
			
//			 tschedule(t.getCompo(),t.tname, current, changed);
			 needscheduler.remove(t.getTid());
			 temp.put(t.tname, changed.toString());
//			 System.out.println(needscheduler.size()+"after update");
			
		}
//		    
	   return temp;
//		
	}


	public static String findTopt(HashMap<String, Integer> priority){
		Entry<String, Integer> maxEntry = null;
		
		for (Entry<String, Integer> entry : priority.entrySet())
		{
		    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
		    {
		        maxEntry = entry;
		    }
		}
		return maxEntry.getKey();
	
	
	}
		

	public static int availableCore(String type, int usage){
		int result = 0;
//		System.out.println("availcore type "+type+" usage "+usage);
		if(type.contains("s"))
			result = 8-usage/10;
		else if(type.contains("m"))
			return 12-usage/7;
		else
			return 16-usage/5;
		
		return result;
	}
	
	
	public static HashMap<String, String>  tschedule(HashMap<String,Component> compo, String tid, HashMap<String, String> current, HashMap<String, String> changed){
//	public static HashMap<String, String> tschedule(HashMap<String,Component> compo, String tid, HashMap<String, String> current, HashMap<String, String> changed){
		HashMap<String, String> scheduleupdate = new HashMap<String, String>();
		HashMap<String, Double> sortedchange = new HashMap<String, Double>();
		HashMap<String, Integer> usage = getUsage();
		int availcore = 0;
		System.out.println("usage now is "+usage);
		
		// model-based scheduler
		sortedchange = (HashMap<String, Double>) sortResult(compo, changed,true);
		
//		 scheduler without sorting the opeartors latency
//		sortedchange = (HashMap<String, Double>) sortResult(compo, changed,false);
		
		
//		System.out.println("sorted result "+sortedchange.size());
		for(String ctid: sortedchange.keySet()){
//			System.out.println("rescheduler for "+ctid);
			String type = changed.get(ctid);
			ArrayList<String> newtype = new ArrayList<String>();
			newtype.add(type+"1");
			newtype.add(type+"2");
//			System.out.println("rescheduler check 2");
			for(int i = 0; i<newtype.size();i++){
//				System.out.println("rescheduler check i"+i+"type "+newtype.get(i));
//				System.out.println("usage "+usage.size()+ "usage contain "+usage.get(newtype.get(i)));
				availcore = availableCore(newtype.get(i), usage.get(newtype.get(i)));
//				System.out.println("rescheduler check availcore"+availcore);
				int now = usage.get(newtype.get(i));
				int update = getUsageRatio(newtype.get(i));
//				System.out.println("rescheduler check 3");
				if(availcore>0 && (now+update<80)){
//					System.out.println(ctid+" moved from "+ current.get(ctid)+ " to "+newtype.get(i));
					scheduleupdate.put(ctid,newtype.get(i));
					int usagenow = usage.get(current.get(ctid));
					int usageupdate = getUsageRatio(current.get(ctid));
					usage.put(current.get(ctid), usagenow-usageupdate);
					usage.put(newtype.get(i), now+update);
					break;
				}
//				System.out.println("not changed schedule for "+ctid);
			}
		}
//		System.out.println("after usage check ,size of current "+current.keySet());
		
		for(String s: current.keySet()){
			if(!scheduleupdate.containsKey(s)&& !(s.equals("[1-1]"))&&(!s.equals("[2-2]")) ){
				scheduleupdate.put(s, current.get(s));
			}
		}
		
		
//		for(String ctid: changed.keySet()){
//			String type = changed.get(ctid);
//			
//			if(usage.containsKey(type+"1")){
//				int now = usage.get(type+"1");
//				int update = getUsageRatio(type);
//				if(now+update<80){
//					result.put(ctid, type+"1");
//					int usagenow = usage.get(current.get(ctid));
//					int usageupdate = getUsageRatio(current.get(ctid));
//					usage.put(current.get(ctid), usagenow-usageupdate);
//					usage.put(type+"1", now+update);
//				}
//			}
//			else if (usage.containsKey(type+"2")){
//				int now = usage.get(type+"2");
//				int update = getUsageRatio(type);
//				if(now+update<80){
//					result.put(ctid, type+"2");
//					int usagenow = usage.get(current.get(ctid));
//					int usageupdate = getUsageRatio(current.get(ctid));
//					usage.put(current.get(ctid), usagenow-usageupdate);
//					usage.put(type+"2", now+update);
//				}
//			}
//			
//			else{
//				System.out.println("cannot rescheduler for "+ ctid +" in "+tid);
//			}
		
//		}
//		System.out.println("reschedule for "+tid+" , "+result);
//		System.out.println("sorted result for "+tid+" , "+sortResult(compo, result));
//		System.out.println("usage once udpated is "+usage);	
		System.out.println("before reschedle "+current.toString());
		System.out.println("after rescheduelr "+scheduleupdate.toString());
		return scheduleupdate;
	}
	
	//sort the thread based on the latency
	/**
	 * 
	 * @param compo
	 * @param temp
	 * @param sorting if sort the operator to schedule
	 * @return
	 */
	public static Map<String, Double> sortResult(HashMap<String, Component> compo, HashMap<String, String> temp, boolean sorting){
		HashMap<String, Double> result = new HashMap<String, Double>();
		
		for(Component c : compo.values()){
			for(String s: c.getThreads().keySet()){
				if(temp.containsKey(s)){
					result.put(s, c.getThreads().get(s).executelatency);
				}
			}
		}
		
	    if(sorting == true)	{	
	    	Map<String, Double> sortedresult = sortByComparator(result);
	    	return sortedresult;
	    }
	    else 
	    	return result;
		
		
	}
	
	private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap)
    {

        List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Double>>()
        {
            public int compare(Entry<String, Double> o1,
                    Entry<String, Double> o2)
            {
                
                    return o2.getValue().compareTo(o1.getValue());
                
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Entry<String, Double> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	public static HashMap<String, Integer> getUsage() {
		return usage;
	}

	public static void setUsage(HashMap<String, Integer> usage) {
		Optimisation.usage = usage;
	}

	// should choose the value that represents the cpu usage by single task that perform on small, medium, large sized vm
	public static int getUsageRatio(String type){
		if(type.contains("s"))
			return 10;
		else if(type.contains("m"))
			return 7;
		else 
			return 5;
	}
	
	
//	//how to update the slot number
//	public static void main(String[] args) {
//		Optimisation op = new Optimisation(4, 4, 4);
//		System.out.println(op.findType(0, 1000, 2000, 1.0));
//	}
	
	public int getNs() {
		return ns;
	}

	public void setNs(int ns) {
		this.ns = ns;
	}

	public int getNm() {
		return nm;
	}

	public void setNm(int nm) {
		this.nm = nm;
	}

	public int getNl() {
		return nl;
	}

	public void setNl(int nl) {
		this.nl = nl;
	}
	
	
	
	
}