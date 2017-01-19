package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

//import clojure.main;

public class StormREST {
	// all topologies 
//	public HashMap<String, Topology> topologies;
//	ArrayList<Topology> topologies;
	// all supervisors
	URL url;
	String hostport;
	HttpURLConnection conn;

	String output;
	BufferedReader br;
	//used for calculate the prob
	long transnext;
	//public static String parallel;
	// in order to get bolt in aspected order
	
 	
	
	public StormREST(String hostport){
		this.hostport = hostport;
//		topologies  = new HashMap<String, Topology>();
		
		this.output = "";
	}
	
	
	public ArrayList<String> makecopy(Set<String> tlist){
		ArrayList<String> result = new ArrayList<String>();
		for(String s:tlist){
			result.add(s);
		}
		return result;
	}
	
//update the running topology, update log file
	public void Topologyget(HashMap<String,Topology> topologies, boolean ini){
//			System.out.println("topology get");
		    Set<String> tlist = topologies.keySet();
		    ArrayList<String> copy = makecopy(tlist);
		    boolean logchange = false;
		    String sen = "";
			Connect("/api/v1/topology/summary");
			//System.out.println("output from server \n");
			try {
				while((output = br.readLine()) != null){
//		System.out.println(output+ "\n");
					JSONParser parser = new JSONParser();
					
					Object obj = parser.parse(output);
					JSONObject jobj = (JSONObject)obj;
					JSONArray topo = (JSONArray) jobj.get("topologies");
					for (int i = 0 ; i< topo.size(); i++){
						obj = topo.get(i);
						jobj = (JSONObject) obj;
					//	String name = (String)jobj.get("name");
						String id = (String)jobj.get("id");
//						if(topologies.containsKey(id)){
//							copy.remove(id);
////							System.out.println("reduce "+id +" in current topology records");
//						}
//						else{
//							logchange = true;
//							String time = Methods.formattime();
//							sen += time + "  add topology "+ id +"\n";
						String name = (String)jobj.get("name");
//							String tanme = (String)jobj.get("name");
						String uptime = (String)jobj.get("uptime");
				//		System.out.println("id is  "+ id);
//					System.out.println(temp.get("name")+ ", "+temp.get("id"));
//						System.out.println(topo.get(i).getClass().getName());
				//		topologies.add(new Topology((String)jobj.get("id")));
						if(!topologies.containsKey(id)){
							logchange = true;
							String time = Methods.formattime();
							sen += time + "  add topology "+ id +"\n";
							Topology t = new Topology(id,name);
							topologies.put(id, t);
						}
						else{
							copy.remove(id);
////						System.out.println("reduce "+id +" in current topology records");
						}
//						System.out.println("tid "+t.tid+ " tname "+ t.tname+ " tshape "+t.shape);
							topologies.get(id).setUptime(uptime);
//							System.out.println("topology get end");
							topologySum(id, ini, topologies);
						
						
					}
//					System.out.println("topologies "+topologies.toString());
				}
				if(copy.size()>0){
					logchange = true;
					String time = Methods.formattime();
					sen += "\n" + time +"  remove topology: ";
					for(String s: copy){
						topologies.remove(s);
						sen += s+" ,";
						}
					}
					
				 if(logchange){
					 Methods.writeFile(sen, "log.txt");
				 }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			System.out.println("values " +topologies.toString());
			conn.disconnect();
			
			
		}

//	public static void main(String[] args) throws InterruptedException, IOException {
//	
//		StormREST sr = new StormREST("http://115.146.85.187:8080");
//
//	//	String freq = args[0];
//	
//		sr.Topologyget();
//		System.out.println(sr.topologies);
////		sr.Topologyinfo();
////		sr.topologySum();
////		for(Topology t: sr.topologies.values()){
////			System.out.println(t.getSpout());
////			System.out.println(t.getBolts());
////		}
////		System.out.println(sr.workers);
//	}

//	// get the active topology info
	public void Topologyinfo(HashMap<String, Topology> topologies){
//		System.out.println("topologyinfo");
//		System.out.println("size "+topologies.size());
		if (topologies.size() == 0)
			System.out.println("no topology is working at the moment");
		else{
			for(String s : topologies.keySet()){
			//	System.out.println("key is "+e.getKey());
//				System.out.println("toinfo "+s);
				topologyworker(s, topologies);
			}
		
		
//			try {
//				File f = new File(Constants.topologyworker);
//				FileWriter fw = new FileWriter(f);
				for(Entry<String, Topology> e : topologies.entrySet()){
					String time = Methods.formattime();
////					fw.write(time +" , "+ e.getKey() + " , "+ e.getValue().tworker.toString()+"\n");
////					fw.flush();
					String sen = e.getKey() + " , "+ e.getValue().tworker.toString()+"\n";
//					System.out.println(sen);
					Methods.writeFile(sen, "tworkers.txt");
				}
//				fw.close();
//			}catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//				}
		}
	}
//	
//	
//	// get the worker information of each topology
	public void topologyworker(String id, HashMap<String, Topology> topologies){
		Connect("/api/v1/topology-workers/"+id);
		try{
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject)obj;
				
				// check the type of return value
				
			    JSONArray topo = (JSONArray) jobj.get("hostPortList");
			   	ArrayList<Executor> temp = new ArrayList<Executor>();
			    for (int i = 0 ; i< topo.size(); i++){
					obj = topo.get(i);
					jobj = (JSONObject) obj;
//					System.out.println(jobj.get("port").getClass().getName());
					String h = (String)jobj.get("host");
					
					Long p = (Long)jobj.get("port");
					Executor e = new Executor(h, p);
					temp.add(e);
//					if(topologies.get(id).getTworker().containsKey((String)jobj.get("host"))){
//						topologies.get(id).getTworker().get((String)jobj.get("host")).add((Long)jobj.get("port"));
//					}
//					else{
//						 ArrayList<Long> ports = new ArrayList<Long>();
//						 ports.add((Long)jobj.get("port"));
//						 topologies.get(id).getTworker().put((String)jobj.get("host"), ports);
//							topologies.put((String)jobj.get("name"), (String)jobj.get("id"));
						}
			    topologies.get(id).setTworker(temp);
					}
//				System.out.println(topologies.get(id));
			}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
	}

	public void Connect(String q){
		try {
			url = new URL(hostport+q);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode() != 200){
				throw new RuntimeException("Failed : http error code"+ conn.getResponseCode());
				}

			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		}
			catch (MalformedURLException e){
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
//	

//	
	public void topologySum(String s, boolean ini, HashMap<String, Topology> topologies){
//		System.out.println("topology size "+topologies.size());
	
//		for(Entry<String, Topology> e: topologies.entrySet()){
//			System.out.println("topology sum");
				topologySpoutInitial(topologies.get(s).compo, s, ini,topologies);
				
//				topologyBoltInitial(s,ini,topologies);
			
				if (ini == true)
					topologies.get(s).initopology();
//				System.out.println(e.getKey() + e.getValue().compo.toString());
//				t.printTopology();
//				System.out.println(s + "after bolts update "+t.compo.toString());
//				System.out.println("com is "+ com.toString());
					
//		}
//		for(Topology t: topologies.values()){
//			System.out.println(t.tid + " , "+ t.toString());
//			t.toString();
//		}
	}
//	
	//get the spout and bolt of each topology
	public void topologySpoutInitial(HashMap<String,Component> com, String tid, boolean ini,HashMap<String, Topology> topologies){
	
		Connect("/api/v1/topology/"+tid);
		JSONArray stats = null;
		JSONArray topo = null;
		JSONArray tooo = null;
		String spoutid ="";
		long emit = 0;
		try{
		
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject)obj;
				
			
			
				// check the type of return value
//				System.out.println(jobj.getClass().getName());
			
				stats = (JSONArray)jobj.get("topologyStats");
				topo = (JSONArray)jobj.get("spouts");
				tooo = (JSONArray)jobj.get("bolts");
			
			    // only allow one spout for each topology
			    for (int i = 0; i<topo.size() ; i++){
//			    System.out.println("get output "+topo);
			        if(ini == true){
			        	obj = topo.get(0);
			        	jobj = (JSONObject) obj;
//					topologies.get(id).getTworker().put((String)jobj.get("host"), (Long)jobj.get("port"));
			//		System.out.println("latency is "+(String)jobj.get("completeLatency"));
//							topologies.put((String)jobj.get("name"), (String)jobj.get("id"));
			        	spoutid = (String)jobj.get("spoutId");
//			    	System.out.println("spout id "+spoutid);
//			        	long thread = updateComponentThread(tid, spoutid, topologies);
			        	long thread = (Long)jobj.get("executors");
//			    	topologies.get(id).setSpout(new Spout(spoutid));
			        	Component c = new Component(spoutid, thread, true);
			    	
			        	com.put(c.cid,c);
			        	topologies.get(tid).setCompo(com);
			        	
			        }
			        else{
			        	
			        	obj = topo.get(0);
			        	jobj = (JSONObject) obj;
			        	spoutid = (String)jobj.get("spoutId");
			        	Long lastemit = (Long)jobj.get("emitted");
			        	Long lasttrans = (Long)jobj.get("transferred");
			        	
			        	
			        
			        	topologies.get(tid).getCompo().get(spoutid).setLast(lastemit, lasttrans);
			        	emit  =lastemit;
//						topologies.get(tid).getCompo().get(spoutid).setTotalprocess(temp);
			        }
//			        updateComponentThread(tid, spoutid,topologies,ini,lastemit);
//			    	System.out.println("add new component "+ topologies.get(tid).getCompo().size());
			    }
			   
			   
			}
			 updateComponentThread(tid, spoutid,topologies,ini,emit,true);
			 topologyBoltInitial(tid, ini, topologies, tooo);
			 updateTopolgoyStats(tid, stats, topologies);
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		conn.disconnect();
	
	}
//	
	public void updateTopolgoyStats(String tid, JSONArray stats, HashMap<String, Topology> topologies){
//		System.out.println("topolgoy stats ini "+tid);

		Object obj = stats.get(0);
	    JSONObject jobj =(JSONObject) obj;
//	 	System.out.println("updatetoplogystats "+jobj.get("emitted").getClass().getName());
	   
		long emit = (Long)jobj.get("emitted");
//		double emittemp = Double.valueOf(Methods.formatter.format(emit));
		String latency = (String)jobj.get("completeLatency");
		
		topologies.get(tid).setSystememit(emit/600.0);
		
		
		topologies.get(tid).setSystemlatency(Double.valueOf(latency));
	}
	public void topologyBoltInitial(String id, boolean ini,HashMap<String, Topology> topologies, JSONArray bolt){
					HashMap<String, Long> compos = new HashMap<String,Long>();
		//			while((output = br.readLine()) != null){
		//				JSONParser parser = new JSONParser();
		//				obj = parser.parse(output);
		//				jobj = (JSONObject)obj;	
		//				
		//				// check the type of return value
		////				System.out.println(jobj.getClass().getName());
		//				JSONArray temp = (JSONArray)jobj.get("bolts");
						JSONArray temp = bolt;
		//				ArrayList<Bolt> b = new ArrayList<Bolt>();
						for (int i = 0 ; i< temp.size(); i++){
							Object obj = temp.get(i);
							JSONObject jobj = (JSONObject) obj;
							
							String boltid = (String)jobj.get("boltId");
//							System.out.println("bolt id "+boltid);
		//					b.add(new Component(boltid));
							if(ini == true){
								long thread = updateComponentThread(id, boltid,topologies,ini,0,false);
//								long thread = (Long)jobj.get("executors");
								compos.put(boltid, thread);
							}
							
							//update the components
							else{
								Long executed = (Long)jobj.get("executed");
								Long emitted = (Long)jobj.get("emitted");
								Long transferred = (Long)jobj.get("transferred");
//								System.out.println("need to check the class type of execute latency for "+boltid);
								String latency = (String)jobj.get("executeLatency");
								Component c =  topologies.get(id).getCompo().get(boltid);
								c.setLast(emitted, transferred);
								c.setExecute(executed);
								
								
								double latencyupdate = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(latency)));
							
								c.updateArr_Ser(latencyupdate, false);
								c.setExeLatency(Double.valueOf(latency));
//								c.updateArr_Ser(numincoming, false);
//								for(String s : c.threads.keySet()){
//									c.threads.get(s).setCompoemit(emitted);
//								}
//								c.updateArr_Ser(numincoming, arr);
								updateComponentThread(id, boltid,topologies,ini,emitted,false);
//								long thread = (Long)jobj.get("executors");
								
//								for(ComponentThread ct: topologies.get(id).getCompo().get(boltid).getThreads().values()){
//									int index = 
//									System.out.println("bolt init transnext is for "+boltid+" , "+transnext);
//									ct.setProb(ct.getAck()*1.0/transnext);
////									System.out.println("bolt ini set"+id+" , "+boltid+"ack "+ct.getAck());
//								}
//								transnext = transferred;
								
							}
		//					Component c = new Component(boltid,thread);
		//					com.add(c);
							
		//					System.out.println("add new bolt "+c.toString());
		//					if (i == 0){
		//						parallel = String.valueOf(jobj.get("executors"));
		//					}
					   }
						if(ini == true){
							Map<String, Long> bolts = orderCompos(compos);
							topologies.get(id).setBolts(bolts);
//							System.out.println("bolts size is "+bolts.size()+"in bolt ini");
						}
//						conn.disconnect();
	}

	public Map<String, Long> orderCompos(HashMap<String, Long> temp){
		Map<String, Long> map = new TreeMap<String, Long>(temp);
	
		return map;
	}
	
	
//	the spout transferrred amount and uptime, to get the arrvial rate of tuples
	//can only call after running 10mins
	public double freqInfo(String tid, String cid, HashMap<String, Topology> topologies){
		Connect("/api/v1/topology/"+tid+"/component/"+cid);
		double result = 0;
		try{
	
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject)obj;
				// check the type of return value
//				System.out.println(jobj.get("executorStats").getClass().getName());
				//spoutSummary or boltStats
				JSONArray temp = (JSONArray)jobj.get("spoutSummary");
				int i = temp.size()-1;
				obj = temp.get(i);
				jobj = (JSONObject) obj;
				Long transferred = (Long)jobj.get("transferred");
				String wind = (String)jobj.get("windowPretty");
				String uptime = topologies.get(tid).getUptime();
				
//				System.out.println("up "+uptime);
			
		//		System.out.println("hour "+hour);
//				String min = uptime.split("m")[0];
//				String sec = uptime.split("s")[0];
				
				if(!uptime.contains("h") && Double.valueOf(uptime.split("m")[0])<10.0){
					result =  freqCal(uptime.split("m")[0],transferred);
					System.out.println("less than 10min");
//					System.out.println("minute "+uptime.split("m")[0]);
				}
				else 
					result = freqCal("10",transferred);
				topologies.get(tid).getCompo().get(cid).setTotalprocess(result);
//			    System.out.println("arr "+ result);
			   }
			
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
		return result;
		
	}
	
	//calculate the arrival rate 
	public double freqCal(String uptime, Long transferred){
//		System.out.println("uptime to freqcAL "+ uptime);
		DecimalFormat formatter = new DecimalFormat("#0.00");
//	    System.out.println(formatter.format(t1*t2));
//		String t = uptime.split("m")[0];
//		Double temp = Double.valueOf(t);
		double result = 0;
		result = Double.valueOf(formatter.format(transferred/(60.0*Double.valueOf(uptime))));
		
	    return result;
	}

	public double processavg(String uptime, double totalprocess){
		double result = 0;
		String h="0";
		String m ="0";
		String s = "0";
		if(uptime.contains("h")){
			h = uptime.split("h")[0];
			uptime = uptime.split("h")[1];
			}
		
//		System.out.println("h "+h+", "+uptime);
		if(uptime.contains("m")){
			m = uptime.split("m")[0];
			uptime = uptime.split("m")[1];
		}
//		System.out.println("m "+m+", "+uptime);
		
		s = uptime.split("s")[0];

//		System.out.println("s "+s);
		long timeinsec = Integer.valueOf(h)*3600+Integer.valueOf(m)*60+Integer.valueOf(s);
//		System.out.println("timeinsec "+timeinsec);
		result = totalprocess*600/timeinsec;
		return result;
	}
	public double serviceRate(String id,HashMap<String, Topology> topologies){
//		System.out.println("data retrival service Rate pre A "+id);
		Connect("/api/v1/topology/" + id);
		double result = 0;
//		System.out.println("data retrival service Rate A");
		try{	
			while((output = br.readLine()) != null){
		
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject)obj;
				// check the type of return value
//				System.out.println(jobj.get("topologyStats").getClass().getName());
				//spoutSummary or boltStats
				Long workers = (Long)jobj.get("workersTotal");
				topologies.get(id).setWorkers(workers);
//				System.out.println("data retrival service Rate B");
//				System.out.println("workers "+workers);
				JSONArray temp = (JSONArray)jobj.get("topologyStats");
				obj = temp.get(0);
				jobj = (JSONObject)obj;
//				String s = (String)jobj.get("windowPretty");
				
				String servicetime = (String)jobj.get("completeLatency");
//				System.out.println("data retrival service Rate C");
//				System.out.println("data retrival service rate service time "+servicetime);
				result = Double.valueOf(Methods.formatter.format(1000/Double.valueOf(servicetime)));
//				System.out.println("service rate "+result);
			}
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
		return result;
	}

	public long updateComponentThread(String tid, String cid,HashMap<String, Topology> topologies, boolean ini, long emitted, boolean spout){
		Connect("/api/v1/topology/"+tid+"/component/"+cid);
		Long result = null;
		HashMap<String,ComponentThread> ctmap = new HashMap<String, ComponentThread>();
		try{
			
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(output);
				JSONObject jobj = (JSONObject)obj;
				result = (Long)jobj.get("executors");
				
				if(ini == true){	
			//	System.out.println(tid+" , "+cid+", "+result);
					return result;
					}
				else{
					JSONArray systemperform ;
					if(spout == false){
						systemperform = (JSONArray)jobj.get("boltStats");
					}
					else{
						systemperform = (JSONArray)jobj.get("spoutSummary");
					}
						JSONArray temp = (JSONArray)jobj.get("executorStats");
				    	updateComponentsys(tid, cid, systemperform, topologies,spout);
//				System.out.println("result size "+temp.size());
					ArrayList<Executor> exe;
					ArrayList<Executor> check = new ArrayList<Executor>();
					
//					System.out.println("executor "+topologies.get(tid).getCompo().toString()+ " cid "+cid);
					if(topologies.get(tid).getCompo().get(cid).getExecutors().size()>0){
						exe = topologies.get(tid).getCompo().get(cid).getExecutors();
			
				//for checking if an executor has been removed
						check.addAll(exe);
						ctmap = topologies.get(tid).getCompo().get(cid).getThreads();
					}
					else{
						exe = new ArrayList<Executor>();
					
						ctmap = new HashMap<String,ComponentThread>();
					}
					for(int i = 0; i<temp.size(); i++){
						obj = temp.get(i);
						jobj = (JSONObject)obj;	
						Long port = (Long)jobj.get("port");
						String host = (String)jobj.get("host");
//						Executor e = new Executor(host,port,tid);
						Executor e = new Executor(host,port);
						if(!exe.contains(e)){
							exe.add(e);
//							topologies.get(tid).getCompo().get(cid).setExecutors(exe);
						}
						if(check.contains(e)){
							check.remove(e);
						}
						ComponentThread ct;
						String ctid = (String)jobj.get("id");
//					Long emit = (Long)jobj.get("emitted");
						long ack;
						if(spout ==false){
							Long execute = (Long)jobj.get("executed");
							String processlatency = (String)jobj.get("processLatency");
							String executelatency = (String)jobj.get("executeLatency");
							ack = (Long)jobj.get("acked");
							if(ctmap.containsKey(ctid)){
								ct = ctmap.get(ctid);
							}
							else{
								ct = new ComponentThread(ctid);
								ctmap.put(ctid, ct);
							}
							ct.updateThread(execute, Double.valueOf(executelatency), Double.valueOf(processlatency), ack,e);
						}
						else{
							Long transfer = (Long)jobj.get("transferred");
							String processlatency = (String)jobj.get("completeLatency");
//							String executelatency = (String)jobj.get("executeLatency");
							ack = (Long)jobj.get("acked");
							if(ctmap.containsKey(ctid)){
								ct = ctmap.get(ctid);
							}
							else{
								ct = new ComponentThread(ctid);
								ctmap.put(ctid, ct);
							}
							ct.updateThread(transfer, Double.valueOf(processlatency), 0.0, ack,e);
						}
//							System.out.println("updatect "+ctid+" , "+emitted);
							
						ct.setCompoemit(emitted);
						if(emitted!=0){
							ct.setProb(ack*1.0/emitted);
//							System.out.println("set prob for "+ctid+" , "+ack*1.0/emitted);
						}
					}
					updateExecutors(tid,cid,check,exe, topologies);
					}
			}
			
				topologies.get(tid).getCompo().get(cid).setThreads(ctmap);
//				System.out.println("check executor "+topologies.get(tid).getCompo().get(cid).threads.toString());
			
			}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		conn.disconnect();	
		
		return result;
	}
	
	public void updateComponentsys(String tid, String cid, JSONArray comstats,HashMap<String, Topology> topologies, boolean spout){
	
		Object obj = comstats.get(0);
		JSONObject jobj = (JSONObject)obj;
//		long executed = (Long)jobj.get("executed");
		String latency;
		if(spout == false)
			latency= (String)jobj.get("executeLatency");
		else
			latency = (String)jobj.get("completeLatency");
//		topologies.get(tid).getSystemcoemit().put(cid, executed/600);
		topologies.get(tid).getSystemcolatency().put(cid, Double.valueOf(latency));
//		System.out.println("update comopnet "+cid+ " latency with "+latency);
	}
	
	public void updateExecutors(String tid, String cid, ArrayList<Executor> check, ArrayList<Executor> update,HashMap<String, Topology> topologies){
		 if(check.size()>0){
			 for(Executor e: check){
				 update.remove(e);
			 }
		 }
		 topologies.get(tid).getCompo().get(cid).setExecutors(update);
	}
	
}

