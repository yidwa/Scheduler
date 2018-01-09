package general;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class StormREST {
	URL url;
	String hostport;
	HttpURLConnection conn;

	String output;
	BufferedReader br;
	// used for calculate the probability
	long transnext;

	public StormREST(String hostport) {
		this.hostport = hostport;
		this.output = "";
	}

//	// make a hard copy of the given list
//	public ArrayList<String> makecopy(Set<String> tlist) {
//		ArrayList<String> result = new ArrayList<String>();
//		for (String s : tlist) {
//			result.add(s);
//		}
//		return result;
//	}

	// update the running topology and their priority, update log file with new
	// and killed topology
//	public void Topologyget(HashMap<String, Topology> topologies, HashMap<String, Integer> priority, boolean ini,
//			ArrayList<PriorityQueue> pq) {
//	public void Topologyget(boolean ini) {
//		System.out.println("calling method topology get with ini "+ini);
//		Set<String> tlist = StormCluster.topologies.keySet();
////		ArrayList<PriorityQueue> queues = pq;
//		ArrayList<String> compare = new ArrayList<String>();
//		// update info
//		
//		boolean logchange = false;
//		boolean topologyini = false;
//		String sen = "";
//		Connect("/api/v1/topology/summary");
//
//		try {
//			while ((output = br.readLine()) != null) {
//				JSONParser parser = new JSONParser();
//				Object obj = parser.parse(output);
//				JSONObject jobj = (JSONObject) obj;
//				JSONArray topo = (JSONArray) jobj.get("topologies");
//				for (int i = 0; i < topo.size(); i++) {
//					obj = topo.get(i);
//					jobj = (JSONObject) obj;
//					String id = (String) jobj.get("id");
//					String name = (String) jobj.get("name");
////					Long uptimeSeconds = (Long) jobj.get("uptimeSeconds");
//					// default priority is 3, the lowest level if it is not
//					// included in the topology name
//					int pri = 3;
//					if (name.contains("_")) {
//						int ind = name.indexOf("_");
//						pri = Integer.valueOf(name.substring(ind + 1, ind + 2));
//					}
////					System.out.println("test , the id now is "+id);
//					// initialize
//					if (ini) {
//						logchange = true;
//						sen += "add topology " + id + " with prioirty " + pri + "\n";
//						Topology t = new Topology(id, name);
//						System.out.println("initialize for  "+id +" : "+StormCluster.topologies.put(id, t));
//						StormCluster.priority.put(id, pri);
//						PriorityQueue tempqueue = StormCluster.queue.get(pri-1);
//						tempqueue.setPrioirty(pri);
//						tempqueue.setSize(tempqueue.getSize()+1);
//						ArrayList<String> n = tempqueue.getNames();
//						n.add(id);
//						tempqueue.setNames(n);
//						topologyini = true;
//						
//					}
//					// update the topology if it is new and remove it from the active list if it is alive.
//					else {
//						
//						System.out.println("it's in update and the "+id+ "is exist? "+StormCluster.topologies.containsKey(id));
//						//not yet added
//						if (!StormCluster.topologies.containsKey(id)) {
//							logchange = true;
//							sen += "add topology " + id + " with prioirty " + pri + "\n";
//							Topology t = new Topology(id, name);
//							System.out.println("add new entry of id "+id +" :"+StormCluster.topologies.put(id, t));
//							System.out.println("now the topologies is in the size of "+StormCluster.topologies.size()+ " and "+StormCluster.topologies.containsKey(id));
//							StormCluster.priority.put(id, pri);
//							PriorityQueue tempqueue = StormCluster.queue.get(pri-1);
//							tempqueue.setSize(tempqueue.getSize()+1);
//							ArrayList<String> n = tempqueue.getNames();
//							n.add(id);
//							tempqueue.setNames(n);
//							topologyini = true;
//							sen+= " now the priority queues for priority "+pri +" is ";
//							for(String s: StormCluster.queue.get(pri-1).getNames()){
//								sen += s+ " , ";
//							}
//							sen+="\n";
//							
//							}
//						// the given topology is still alive
//						else{
//							compare.add(id);
//							topologyini = false;
//						}
//					}
////					StormCluster.topologies.get(id).setUptime(uptimeSeconds);
//					topologySum(id, topologyini, StormCluster.topologies);
//				}
//			}
//			if(ini == false){
//				for(String s: StormCluster.topologies.keySet()){
//					if(!compare.contains(s)){
//						logchange = true;
//						sen += "  remove topology: ";
//						StormCluster.topologies.remove(s);
//						System.out.println("remove topology "+s);
//						int p = StormCluster.priority.get(s);
//						StormCluster.queue.get(p-1).size--;
//						StormCluster.queue.get(p-1).names.remove(s);
//						StormCluster.queue.get(p-1).hosts = new ArrayList<>();
//						sen += s + " ,";
//					}
//				}
//			}
////			if (copy.size() > 0) {
////				logchange = true;
////				sen += "  remove topology: ";
////				for (String s : copy) {
////					StormCluster.topologies.remove(s);
////					int p = StormCluster.priority.get(s);
////					StormCluster.queue.get(p-1).size--;
////					StormCluster.queue.get(p-1).names.remove(s);
////					StormCluster.queue.get(p-1).hosts = new ArrayList<>();
////					sen += s + " ,";
//////					System.out.println("the priority queus has been updated for priority "+p+ " with remove of "+ s);
////				}
////			}
//			CentralControl.setPriority(StormCluster.priority);
//			//testing purpose
////			System.out.println("testing the prority queue");
////			for(PriorityQueue pp :StormCluster.queue){
////				System.out.println(pp.prioirty+" , "+pp.size+" , "+pp.names+" , "+pp.hosts.toString()+"\n");
////			}
//			if (logchange) {
//				Methods.writeFile(sen, "log.txt", true);	
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		conn.disconnect();
//	}

	// try to rewrite
	public void Topologyget(boolean ini) {
//		Set<String> tlist = StormCluster.topologies.keySet();
//		ArrayList<PriorityQueue> queues = pq;
		ArrayList<String> compare = new ArrayList<String>();
		// update info
		
	
		boolean topologyini = true;
		String sen = "";
		Connect("/api/v1/topology/summary");

		try {
			while ((output = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject) obj;
				JSONArray topo = (JSONArray) jobj.get("topologies");
				String id = "";
				if(ini){
					for (int i = 0; i < topo.size(); i++) {
						obj = topo.get(i);
						jobj = (JSONObject) obj;
						id = (String) jobj.get("id");
						String name = (String) jobj.get("name");
						//					Long uptimeSeconds = (Long) jobj.get("uptimeSeconds");
						// default priority is 3, the lowest level if it is not
						// included in the topology name
						int pri = 3;
						if (name.contains("_")) {
							int ind = name.indexOf("_");
							pri = Integer.valueOf(name.substring(ind + 1, ind + 2));
						}
						
						sen += "add topology " + id + " with prioirty " + pri + "\n";
						Topology t = new Topology(id, name);
						System.out.println("add new entry of id "+id +" :"+StormCluster.topologies.put(id, t));
						StormCluster.priority.put(id, pri);
						PriorityQueue tempqueue = StormCluster.queue.get(pri-1);
						tempqueue.setPrioirty(pri);
						tempqueue.setSize(tempqueue.getSize()+1);
						ArrayList<String> n = tempqueue.getNames();
						n.add(id);
						tempqueue.setNames(n);
						topologyini = true;
						
					}
					
					
				}
					// update the topology if it is new and remove it from the active list if it is alive.
				else{
					compare = new ArrayList<>();
					for (int i = 0; i < topo.size(); i++) {
						obj = topo.get(i);
						jobj = (JSONObject) obj;
						id = (String) jobj.get("id");
						String name = (String) jobj.get("name");
						int pri = 3;
						if (name.contains("_")) {
							int ind = name.indexOf("_");
							pri = Integer.valueOf(name.substring(ind + 1, ind + 2));
						}
					
						System.out.println("it's in update and the "+id+ "is exist? "+StormCluster.topologies.containsKey(id));
						//not yet added
						if (StormCluster.topologies.containsKey(id)) {
							System.out.println("id "+id+" is already existed");
							compare.add(id);
							topologyini = false;	
						}
						// the given topology is still alive
						else{
							sen += "add topology " + id + " with prioirty " + pri + "\n";
							Topology t = new Topology(id, name);
							System.out.println("add new entry of id "+id +" :"+StormCluster.topologies.put(id, t));
							StormCluster.priority.put(id, pri);
							PriorityQueue tempqueue = StormCluster.queue.get(pri-1);
							tempqueue.setSize(tempqueue.getSize()+1);
							ArrayList<String> n = tempqueue.getNames();
							n.add(id);
							tempqueue.setNames(n);
							topologyini = true;
							sen+= " now the priority queues for priority "+pri +" is ";
							for(String s: StormCluster.queue.get(pri-1).getNames()){
								sen += s+ " , ";
							}
							sen+="\n";
							topologyini = true;
							compare.add(id);
						}
						topologySum(id, topologyini, StormCluster.topologies);
					}
				}
				
			}
		    
			if(ini == false){
				String temp ="";
				boolean rem = false;
				for(String s: StormCluster.topologies.keySet()){
					if(!compare.contains(s)){
						rem = true;
						temp += s+" , ";
						StormCluster.topologies.remove(s);
						System.out.println("remove topology "+s);
						int p = StormCluster.priority.get(s);
						StormCluster.queue.get(p-1).size--;
						StormCluster.queue.get(p-1).names.remove(s);
						StormCluster.queue.get(p-1).hosts = new ArrayList<>();
					}
				}
				if(rem){
					sen += "remove topology : "+temp;
					sen += "\n";
				}
			}
			CentralControl.setPriority(StormCluster.priority);
			//testing purpose
//			System.out.println("testing the prority queue");
//			for(PriorityQueue pp :StormCluster.queue){
//				System.out.println(pp.prioirty+" , "+pp.size+" , "+pp.names+" , "+pp.hosts.toString()+"\n");
//			}
	
			System.out.println("calling write file : "+sen);
			Methods.writeFile(sen, "log.txt", true);	
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
	}
	
	
	/**
	 * collect the worker information of each topology and write the information to a specific file
	 * @param topologies, the collection of active topologies
	 */
	public void Topologyinfo(HashMap<String, Topology> topologies) {
		if (topologies.size() == 0)
			System.out.println("no topology is working at the moment");
		else {
			for (String tname : topologies.keySet()) {
				topologyworker(tname, topologies);
			}
			//testing purpose
//			for(String tname : topologies.keySet()){
//				System.out.println("topology :"+tname+":");
//				for(Executor e: topologies.get(tname).getTworker()){
//					System.out.println(e.getHost()+" , "+e.getPort()+",");
//				}
//					
//			}
			String sen="";
			for (Entry<String, Topology> e : topologies.entrySet()) {
				sen += e.getKey() + " , " + e.getValue().tworker.toString() + "\n";
			}
			Methods.writeFile(sen, "tworkers.txt", true);
		}
	}


	/**
	 * Collect the worker information of each component
	 * @param id, topology name
	 * @param topologies
	 */
	public void topologyworker(String id, HashMap<String, Topology> topologies) {
		Connect("/api/v1/topology-workers/" + id);
		try {
			while ((output = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject) obj;
				JSONArray topo = (JSONArray) jobj.get("hostPortList");
				//active host + port for the given topology
				ArrayList<Executor> temp = new ArrayList<Executor>();
				
				for (int i = 0; i < topo.size(); i++) {
					obj = topo.get(i);
					jobj = (JSONObject) obj;
					String h = (String) jobj.get("host");
					Long p = (Long) jobj.get("port");
					// create executor for each port 
					Executor e = new Executor(h, p);
					temp.add(e);
				}
//				System.out.println("now update the Tworker for "+id+" with "+temp.size());
				topologies.get(id).setTworker(temp);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
	}

	/**
	 * Connect to the REST API and start to collect information
	 * @param the command to execute
	 */
	public void Connect(String q) {
		try {
			url = new URL(hostport + q);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : http error code" + conn.getResponseCode());
			}

			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * initialize or update the topology status
	 * @param s , topology name
	 * @param ini, is new or not 
	 * @param topologies, the recored topology list
	 */
	public void topologySum(String s, boolean ini, HashMap<String, Topology> topologies) {
		topologySpoutInitial(topologies.get(s).compo, s, ini, topologies);
	}

	/**
	 * initialize or update each component of the topology and its corresponding status
	 * @param com, the component collection for the given topology name
	 * @param tid, topology name
	 * @param ini, if it is new or old one
	 * @param topologies, the topology collection
	 */
	public void topologySpoutInitial(HashMap<String, Component> com, String tid, boolean ini,
			HashMap<String, Topology> topologies) {
		Connect("/api/v1/topology/" + tid);
		JSONArray stats = null;
		JSONArray topo = null;
		JSONArray tooo = null;
		String spoutid = "";
		long emit = 0;
		try {
			while ((output = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject) obj;
				// check the type of return value
				// System.out.println(jobj.getClass().getName());
				stats = (JSONArray) jobj.get("topologyStats");
				topo = (JSONArray) jobj.get("spouts");
				tooo = (JSONArray) jobj.get("bolts");
				if (topo.size() > 0) {
					
					obj = topo.get(0);
					jobj = (JSONObject) obj;
					spoutid = (String) jobj.get("spoutId");
					long thread = (Long) jobj.get("executors");
					
					if(topologies.get(tid).getCompo().size()>0 && topologies.get(tid).getCompo().containsKey(spoutid)){
						Long lastemit = (Long) jobj.get("emitted");
						Long lasttrans = (Long) jobj.get("transferred");
						topologies.get(tid).getCompo().get(spoutid).setLast(lastemit, lasttrans);
						emit = lastemit;
						
					}
					else{
						Component c = new Component(spoutid, thread, true);
						com.put(c.cid, c);
						topologies.get(tid).setCompo(com);
					}
//					for (int i = 0; i < topo.size(); i++) {
//						if (ini == true) {
//							//get the spout
//							obj = topo.get(0);
//							jobj = (JSONObject) obj;
//							spoutid = (String) jobj.get("spoutId");
//							long thread = (Long) jobj.get("executors");
//							Component c = new Component(spoutid, thread, true);
////							long failed = (Long) jobj.get("failed");
////							topologies.get(tid).setFailed(failed);
//							com.put(c.cid, c);
//							topologies.get(tid).setCompo(com);
//							System.out.println("update the spout with id "+c.cid);
//
//						} else {
//							obj = topo.get(0);
//							jobj = (JSONObject) obj;
//							spoutid = (String) jobj.get("spoutId");
//							Long lastemit = (Long) jobj.get("emitted");
//							Long lasttrans = (Long) jobj.get("transferred");
//							topologies.get(tid).getCompo().get(spoutid).setLast(lastemit, lasttrans);
//							emit = lastemit;
//						}
//					}
					updateComponentThread(tid, spoutid, topologies, ini, emit, true);
					topologyBoltInitial(tid, ini, topologies, tooo);
					updateTopolgoyStats(tid, stats, topologies);
					if (ini == true){
						topologies.get(tid).initopology();
					}
				}
				// if the component are not initialized yet, just remove the
				// topology and update later
				else {
//					topologies.remove(tid);
					System.out.println("the topology "+tid+" is not initialized yet");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		 conn.disconnect();
	}

	/**
	 * update the topology status, including the latency and emit number and ack and fail amount
	 * both all time values and 10min values are collected can be used for further analysis
	 * @param tid
	 * @param stats
	 * @param topologies
	 */
	public void updateTopolgoyStats(String tid, JSONArray stats, HashMap<String, Topology> topologies) {
//		long emitall = 0;
		long emitlatest = 0;
		String latencyall = "";
//		String latencylatest = "";
		long ackall = 0;
//		long acklatest = 0;
//		long failall = 0;
//		long faillatest = 0;
		
		for (int i = 0; i < stats.size(); i++) {
			Object obj = stats.get(i);
			JSONObject jobj = (JSONObject) obj;
			String timeframe = (String) jobj.get("window");
			if(timeframe.contains("all-time")){
//				emitall = (Long)jobj.get("emitted");
				latencyall = (String)jobj.get("completeLatency");
				ackall = (Long) jobj.get("acked");
//				if(jobj.get("failed")!= null)
//					failall = (Long) jobj.get("failed");
				}
				else if(timeframe.equals("600")){
					emitlatest = (Long)jobj.get("emitted");
//					latencylatest = (String)jobj.get("completeLatency");
//					acklatest = (Long) jobj.get("acked");
//					if(jobj.get("failed")!= null)
//						faillatest = (Long) jobj.get("failed");
				}
		}
		Topology t = topologies.get(tid);
		t.setSystememit(emitlatest / 600.0);
		t.setSystemlatency(Double.valueOf(latencyall));
		double rate = (t.getFailed() * 1.0) / (ackall + t.getFailed());
		t.setFailrate(Methods.formatter.format(rate));
	}

	/**
	 * initialize the bolt
	 * @param id
	 * @param ini
	 * @param topologies
	 * @param bolt , the array bolts collected from REST API
	 */
	public void topologyBoltInitial(String id, boolean ini, HashMap<String, Topology> topologies, JSONArray bolt) {
		HashMap<String, Long> compos = new HashMap<String, Long>();
		boolean needreorder = false;
		JSONArray temp = bolt;
		for (int i = 0; i < temp.size(); i++) {
			Object obj = temp.get(i);
			JSONObject jobj = (JSONObject) obj;
			String boltid = (String) jobj.get("boltId");
//			if (ini == true) {
//				long thread = updateComponentThread(id, boltid, topologies, ini, 0, false);
//				compos.put(boltid, thread);
//				System.out.println("bolt "+boltid+" finished ini");
//			}
//			// update the components
//			else {
//				Long executed = (Long) jobj.get("executed");
//				Long emitted = (Long) jobj.get("emitted");
//				Long transferred = (Long) jobj.get("transferred");
//				String latency = (String) jobj.get("executeLatency");
//				String processlat = (String) jobj.get("processLatency");
//				Component c = topologies.get(id).getCompo().get(boltid);
//				c.setLast(emitted, transferred);
////				c.setExecute(executed);
//
//				double latencyupdate = Double.valueOf(Methods.formatter.format(1000 / Double.valueOf(latency)));
//				c.updateArr_Ser(latencyupdate, false);
//				c.setExeLatency(Double.valueOf(latency));
//				c.setProcLatency(Double.valueOf(processlat));
//				updateComponentThread(id, boltid, topologies, ini, emitted, false);
//				System.out.println("bolt "+boltid+" finished update");
//			}
			if(topologies.get(id).getCompo().size()>0 && topologies.get(id).getCompo().containsKey(boltid)){
				Long executed = (Long) jobj.get("executed");
				Long emitted = (Long) jobj.get("emitted");
				Long transferred = (Long) jobj.get("transferred");
				String latency = (String) jobj.get("executeLatency");
				String processlat = (String) jobj.get("processLatency");
				Component c = topologies.get(id).getCompo().get(boltid);
				c.setLast(emitted, transferred);
//				c.setExecute(executed);
//
				double latencyupdate = Double.valueOf(Methods.formatter.format(1000 / Double.valueOf(latency)));
				c.updateArr_Ser(latencyupdate, false);
				c.setExeLatency(Double.valueOf(latency));
				c.setProcLatency(Double.valueOf(processlat));
				updateComponentThread(id, boltid, topologies, ini, emitted, false);
				
			}
			else{
				long thread = updateComponentThread(id, boltid, topologies, ini, 0, false);
				compos.put(boltid, thread);
				needreorder = true;
			}
		}
		if (needreorder) {
			Map<String, Long> bolts = orderCompos(compos);
			topologies.get(id).setBolts(bolts);
		}
		// conn.disconnect();
	}

	/**
	 * ordering the bolts according to their name 
	 * @param temp
	 * @return
	 */
	public Map<String, Long> orderCompos(HashMap<String, Long> temp) {
		Map<String, Long> map = new TreeMap<String, Long>(temp);
		return map;
	}

	// the spout transferrred amount and uptime, to get the arrvial rate of
	// tuples
	// can only call after running 10mins
	
	/**
	 * get the spout information 
	 * @param tid
	 * @param cid   spout id 
	 * @param topologies
	 * @return
	 */

	
	//temporarily removed, since something wrong with the emit rate retrieval
//	public double freqInfo(String tid, String cid, HashMap<String, Topology> topologies) {
//		Connect("/api/v1/topology/" + tid + "/component/" + cid);
//		double result = 0;
////		Object objj= null;
//		JSONObject jobjj = null;
//		try {
//			while ((output = br.readLine()) != null) {
//				JSONParser parser = new JSONParser();
//				Object obj = parser.parse(output);
//				JSONObject jobj = (JSONObject) obj;
//				JSONArray temp = (JSONArray) jobj.get("spoutSummary");
//				search:
//					for(int i = 0; i<temp.size(); i++){
//						Object o = temp.get(i);
//						JSONObject jo = (JSONObject) o;
//						String timeframe = (String) jo.get("window");
//						if(timeframe.equals("600")){
////							objj = o;
//							jobjj =jo;
//							break search;
//						}
//				}
//				Long emitted = (Long) jobjj.get("emitted");
//				Long uptime = 
//				System.out.println("get uptime for "+tid+" inside freqiNfo "+uptime);
//				if(uptime<600)
//					result = freqCal(uptime, emitted);
//				else
//					result = freqCal((long)600, emitted);
//				
////				System.out.println("freqcal "+ "uptime "+ uptime+ " emitted " + emitted+ " , "+result);
////				System.out.println("the result is inside freqInfo is "+result);
//				topologies.get(tid).getCompo().get(cid).setTotalprocess(result);
////				System.out.println("the result after setting is inside freqInfo is "+result);
//			}
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		conn.disconnect();
//		return result;
//
//	}

	public double freqInfo(String tid, String cid, HashMap<String, Topology> topologies) {
		Connect("/api/v1/topology/" + tid);
		double result = 0;
//		Object objj= null;
		JSONObject jobjj = null;
		try {
			while ((output = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject) obj;
//				jobj.getClass().getName()
				String uptimetext = (String)jobj.get("uptime"); 
				JSONArray temp = (JSONArray) jobj.get("topologyStats");
				search:
					for(int i = 0; i<temp.size(); i++){
						Object o = temp.get(i);
						JSONObject jo = (JSONObject) o;
						String timeframe = (String) jo.get("window");
						if(timeframe.equals("600")){
//							objj = o;
							jobjj =jo;
							break search;
						}
				}
				Long emitted = (Long) jobjj.get("emitted");
				Long uptime = timeConvert(uptimetext);
//				System.out.println("get uptime for "+tid+" inside freqiNfo "+uptime+ " , emitted "+emitted);
				if(uptime<600)
					result = freqCal(uptime, emitted);
				else
					result = freqCal((long)600, emitted);
				
//				System.out.println("freqcal "+ "uptime "+ uptime+ " emitted " + emitted+ " , "+result);
//				System.out.println("the result is inside freqInfo is "+result);
				topologies.get(tid).getCompo().get(cid).setTotalprocess(result);
//				System.out.println("the result after setting is inside freqInfo is "+result);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
		return result;

	}
	/**
	 * convert the up time from sting to long value
	 * @param timetext
	 * @return
	 */
	public long timeConvert(String timetext){
		String temptext = timetext;
		long hour = 0;
		long min = 0;
		long sec = 0;
	
		if(temptext.contains("h")){
			String[] ht = temptext.split("h\\s");
			hour = Long.parseLong(ht[0]);
			temptext = ht[1];
		}
		if(temptext.contains("m")){
			String[] mt = temptext.split("m\\s");
			min = Long.parseLong(mt[0]);
			temptext = mt[1];
		}
		if(temptext.contains("s")){
			String[] st = temptext.split("s");
			sec = Long.parseLong(st[0]);
		}	
		return hour*3600+min*60+sec;
	}
	

	
	/**
	 * calculate the emit rate 
	 * @param uptime
	 * @param emitted
	 * @return
	 */
	public double freqCal(Long uptime, Long emitted) {
//		DecimalFormat formatter = new DecimalFormat("#0.00");
		double result = 0;
//		result = Double.valueOf(formatter.format(emitted / uptime));
//		System.out.println("inside freqCal "+uptime+ " , emitted "+emitted);
		// update the result to show the arrvial time in ms
		result = Double.valueOf(Methods.formatter.format(uptime*1000.0 / emitted));
		return result;
	}

	/**
	 * Calculate the service rate, where the service time include the time spent on buffer and ack 
	 * @param id
	 * @param topologies
	 * @return
	 */

	public double serviceRate(String id, HashMap<String, Topology> topologies) {
		
		Connect("/api/v1/topology/" + id);
		double result = 0;
		try {
			while ((output = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject) obj;
				// spoutSummary or boltStats
				Long workers = (Long) jobj.get("workersTotal");
				//set the worker number for each topology
				topologies.get(id).setWorkers(workers);
				JSONArray temp = (JSONArray) jobj.get("topologyStats");
				obj = temp.get(0);
				jobj = (JSONObject) obj;
				String servicetime = (String) jobj.get("completeLatency");
				result = Double.valueOf(Methods.formatter.format(1000 / Double.valueOf(servicetime)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
		return result;
	}

	
	/**
	 * get the entire execution time for each component in a topology 
	 */
	 public double executionTotaltime(String id, HashMap<String, Topology> topologies, PriorityQueue pq) {
			
			Connect("/api/v1/topology/" + id);
			double result = 0;
			try {
				while ((output = br.readLine()) != null) {
					JSONParser parser = new JSONParser();
					Object obj = parser.parse(output);
					JSONObject jobj = (JSONObject) obj;
					// spoutSummary or boltStats
					Long workers = (Long) jobj.get("workersTotal");
					//set the worker number for each topology
					topologies.get(id).setWorkers(workers);
					JSONArray temp = (JSONArray) jobj.get("topologyStats");
					Object objt = temp.get(0);
					JSONObject jobjt = (JSONObject) objt;
					String complete = (String) jobjt.get("completeLatency");
					double completelat = Double.valueOf(complete);
					
					HashMap<String,Double> buffer = pq.getBuffertime();
					JSONArray temparray = (JSONArray) jobj.get("bolts");
					double sumlatency = 0.0;
					for(int i = 0 ; i<temparray.size() ; i++){
						Object o = temparray.get(i);
						JSONObject jo = (JSONObject) o;
						String templatencyupdate = (String)jo.get("executeLatency");
//						System.out.println(jo.get("executeLatency").getClass().getName());
						sumlatency += Double.valueOf(templatencyupdate);
//						System.out.println("inside execution total time in dataretvial for templatencyupdate "+ templatencyupdate);
//					
						//update the service time in ms
						result = Double.valueOf(sumlatency);
//						result = Double.valueOf(Methods.formatter.format(1000 / Double.valueOf(sumlatency)));
					}
					buffer.put(id,completelat-sumlatency);
					System.out.println("the complete latency is "+completelat+" , the execute latency is "+sumlatency+" , the buffer time is "+(completelat-sumlatency));
					sumlatency = 0;
//					obj = temp.get(0);
//					jobj = (JSONObject) obj;
//					String servicetime = (String) jobj.get("executeLatency");
//					result = Double.valueOf(Methods.formatter.format(1000 / Double.valueOf(servicetime)));
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conn.disconnect();
			return result;
		}
	
	/**
	 * Collect the latest information for each component thread
	 * @param tid
	 * @param cid
	 * @param topologies
	 * @param ini
	 * @param emitted
	 * @param spout
	 * @return the number of thread for the given component
	 */
	public long updateComponentThread(String tid, String cid, HashMap<String, Topology> topologies, boolean ini,
			long emitted, boolean spout) {
		Connect("/api/v1/topology/" + tid + "/component/" + cid);
		Long result = null;
		HashMap<String, ComponentThread> ctmap = new HashMap<String, ComponentThread>();
		try {
			while ((output = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject) obj;
				result = (Long) jobj.get("executors");
				if (ini) {
					return result;
				}
				else {
					JSONArray systemperform;
					// it is bolt 
					if (spout == false) {
						systemperform = (JSONArray) jobj.get("boltStats");
					} else {
						systemperform = (JSONArray) jobj.get("spoutSummary");
					}
					JSONArray temp = (JSONArray) jobj.get("executorStats");
					updateComponentsys(tid, cid, systemperform, topologies, spout);
					ArrayList<Executor> exe=new ArrayList<Executor>();
					ArrayList<Executor> check = new ArrayList<Executor>();

					if (topologies.get(tid).getCompo().containsKey(cid)){
						if(topologies.get(tid).getCompo().get(cid).getExecutors().size() > 0) {
							exe = topologies.get(tid).getCompo().get(cid).getExecutors();
							// for checking if an executor has been removed
							check.addAll(exe);
							ctmap = topologies.get(tid).getCompo().get(cid).getThreads();
						} 
					}else {
						
						ctmap = new HashMap<String, ComponentThread>();
					}
					for (int i = 0; i < temp.size(); i++) {
						obj = temp.get(i);
						jobj = (JSONObject) obj;
						Long port = (Long) jobj.get("port");
						String host = (String) jobj.get("host");
						Executor e = new Executor(host, port);
						if (!exe.contains(e)) {
							exe.add(e);
						}
						if (check.contains(e)) {
							check.remove(e);
						}
						ComponentThread ct;
						String ctid = (String) jobj.get("id");
						long ack;
						if (spout == false) {
							Long execute = (Long) jobj.get("executed");
							String processlatency = (String) jobj.get("processLatency");
							String executelatency = (String) jobj.get("executeLatency");
							ack = (Long) jobj.get("acked");
							if (ctmap.containsKey(ctid)) {
								ct = ctmap.get(ctid);
							} else {
								ct = new ComponentThread(cid, ctid);
								ctmap.put(ctid, ct);
							}
							ct.updateThread(execute, Double.valueOf(executelatency), Double.valueOf(processlatency),
									ack, e);
						} else {
							Long transfer = (Long) jobj.get("transferred");
							String processlatency = (String) jobj.get("completeLatency");
							ack = (Long) jobj.get("acked");
							if (ctmap.containsKey(ctid)) {
								ct = ctmap.get(ctid);
							} else {
								ct = new ComponentThread(cid, ctid);
								ctmap.put(ctid, ct);
							}
							ct.updateThread(transfer, Double.valueOf(processlatency), 0.0, ack, e);
						}
						ct.setCompoemit(emitted);
						if (emitted != 0) {
							ct.setProb(ack * 1.0 / emitted);
						}
						topologies.get(tid).getCschedule().put(ctid, host);
					}
					updateExecutors(tid, cid, check, exe, topologies);
				}
			}
//			System.out.println("threads upsdated finsh inside");
			if(topologies.get(tid).getCompo().containsKey(cid))
				topologies.get(tid).getCompo().get(cid).setThreads(ctmap);
		
			// System.out.println("check executor
			// "+topologies.get(tid).getCompo().get(cid).threads.toString());

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// conn.disconnect();

		return result;
	}

	/**
	 * update the component data for all time and latest time window
	 * can be further extended if necessary, the data has been stored in the valuealltime and valuelatest
	 * @param tid
	 * @param cid
	 * @param comstats
	 * @param topologies
	 * @param spout
	 */
	public void updateComponentsys(String tid, String cid, JSONArray comstats, HashMap<String, Topology> topologies,
			boolean spout) {
		//value collected for all the time
		Map<String,String> valuealltime = new HashMap<String,String>();
		//value collected for the latest 10min
		Map<String,String> valuelatest = new HashMap<String,String>();
		// get the all time value and the latest 10min value
		String cl ="";
		for (int i = 0; i < comstats.size(); i++) {
			Object obj = comstats.get(i);
			JSONObject jobj = (JSONObject) obj;
			String timeframe = (String) jobj.get("window");
			if((timeframe.contains("all-time")) || (timeframe.equals("600"))){
				Long emitted = (Long)jobj.get("emitted");
				Long transferred = (Long)jobj.get("transferred");
				Long acked = (Long)jobj.get("acked");
				Long failed = (Long)jobj.get("failed");
				if(spout)
					cl = (String)jobj.get("completeLatency");
				else 
					cl = (String)jobj.get("executeLatency");
				if(timeframe.contains("all-time")){
					valuealltime.put("emit", String.valueOf(emitted));
					valuealltime.put("tranf", String.valueOf(transferred));
					valuealltime.put("acked", String.valueOf(acked));
					valuealltime.put("failed", String.valueOf(failed));
					valuealltime.put("latency", cl);
				}
				else{
					valuelatest.put("emit", String.valueOf(emitted));
					valuelatest.put("tranf", String.valueOf(transferred));
					valuelatest.put("acked", String.valueOf(acked));
					valuelatest.put("failed", String.valueOf(failed));
					valuelatest.put("latency", cl);
				}
			}
		}
		if(topologies.get(tid).getCompo().containsKey(cid))
			topologies.get(tid).getCompo().get(cid).setValuealltime(valuealltime);
		topologies.get(tid).getSystemcolatency().put(cid, Double.valueOf(cl));

	}

	/**
	 * remove the no-alive executors and update
	 * @param tid
	 * @param cid
	 * @param check  the list of no longer alive executor
	 * @param update  the list of original executor and new ones
	 * @param topologies
	 */
	public void updateExecutors(String tid, String cid, ArrayList<Executor> check, ArrayList<Executor> update,
			HashMap<String, Topology> topologies) {
//		for(Executor e: check){
//			System.out.print(e.host+" , "+e.port+" ; ");
//		}
//		System.out.println();
//		System.out.println("udpate is ");
//		for(Executor e:update){
//			System.out.print(e.host+" , "+e.port+" ; ");
//		}
//		System.out.println();
		if (check.size() > 0) {
			for (Executor e : check) {
				update.remove(e);
			}
		}
		if(topologies.get(tid).getCompo().containsKey(cid)){
			topologies.get(tid).getCompo().get(cid).setExecutors(update);
		}
//		System.out.println("finsh updateing executors inside");
	}

	
//	
//	/**
//	 * initialize or update the priority queue, with 1 for large , 2 for medium and 3 for small
//	 * @param queue
//	 */
//	public void getQueue(ArrayList<PriorityQueue> queue){
//			ArrayList<PriorityQueue> qu = queue;
////		PriorityQueue q1 =  qu.get(0);
////		PriorityQueue q2 = qu.get(1);
////		PriorityQueue q3 = qu.get(2);
//		
//		ArrayList<ArrayList<String>> ini = new ArrayList<>();
//		for(int i = 0 ;i <=2 ; i++){
//			ini.add(new ArrayList<String>());
//		}
//			
//		Connect("/api/v1/supervisor/summary");
//
//		try {
//			while ((output = br.readLine()) != null) {
//				JSONParser parser = new JSONParser();
//				Object obj = parser.parse(output);
//				JSONObject jobj = (JSONObject) obj;
//				JSONArray topo = (JSONArray) jobj.get("supervisors");
//				for (int i = 0; i < topo.size(); i++) {
//					obj = topo.get(i);
//					jobj = (JSONObject) obj;
//					String host = (String) jobj.get("host");
//					if(host.contains("l")){
//							ini.get(0).add(host);
//						}
//					else if(host.contains("m")){
//							ini.get(1).add(host);
//					}
//					else if(host.contains("s")){
//							ini.get(2).add(host);
//					}
//				}
//			}
//			
//		}
//		catch (IOException e) {
//			e.printStackTrace();
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		for(int i = 0 ; i<3 ; i++){
//			PriorityQueue p = queue.get(i);
//			p.setNames(ini.get(i));
//			p.setSize(ini.get(i).size());
//		}
//	}
//	
//	/**
//	 * build the existing queued host so that remove the ones no longer alive
//	 * @param q1
//	 * @param q2
//	 * @param q3
//	 * @return
//	 */
//	public ArrayList<ArrayList<String>> getExiqueue(PriorityQueue q1, PriorityQueue q2, PriorityQueue q3){
//		ArrayList<String> n1 = q1.getNames();
//		ArrayList<String> n2 = q2.getNames();
//		ArrayList<String> n3 = q3.getNames();
//		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
//		result.add(n1);
//		result.add(n2);
//		result.add(n3);
//		return result;
//	}
//	

//	 public static void main(String[] args) throws InterruptedException, IOException {
//	
//	 StormREST sr = new StormREST("http://115.146.86.60:8080");
//	 PriorityQueue q1 = new PriorityQueue(1, 0, new ArrayList<String>());
//	 PriorityQueue q2 = new PriorityQueue(2, 0, new ArrayList<String>());
//	 PriorityQueue q3 = new PriorityQueue(3, 0, new ArrayList<String>());
//	 // String freq = args[0];
//	 ArrayList<PriorityQueue> queue = new ArrayList<PriorityQueue>();
//	 queue.add(q1);
//	 queue.add(q2);
//	 queue.add(q3);
//	 sr.getQueue(queue);
////	 sr.Topologyget();
//	 for(PriorityQueue pq : queue){
//		 System.out.println(pq.toString());
//	 }
	// sr.Topologyinfo();
	// sr.topologySum();
	// for(Topology t: sr.topologies.values()){
	// System.out.println(t.getSpout());
	// System.out.println(t.getBolts());
	// }
	// System.out.println(sr.workers);
//	 }
}
