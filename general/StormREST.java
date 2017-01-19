package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import clojure.main;

public class StormREST {
	// all topologies 
	HashMap<String, Topology> topologies;
//	ArrayList<Topology> topologies;
	// all supervisors
	HashMap<String, Supervisor> workers;
	URL url;
	String hostport;
	HttpURLConnection conn;
	Object obj;
	JSONObject jobj;
	String output;
	BufferedReader br;
	//public static String parallel;
	
	public StormREST(String hostport){
		this.hostport = hostport;
		topologies  = new HashMap<String, Topology>();
		this.obj = null;
		this.jobj = null;
		this.output = "";
	}
	
	public void Topologyget(){
			Connect("/api/v1/topology/summary");
			//System.out.println("output from server \n");
			try {
				while((output = br.readLine()) != null){
//		System.out.println(output+ "\n");
					JSONParser parser = new JSONParser();
					
					obj = parser.parse(output);
					jobj = (JSONObject)obj;
					JSONArray topo = (JSONArray) jobj.get("topologies");
					for (int i = 0 ; i< topo.size(); i++){
						obj = topo.get(i);
						jobj = (JSONObject) obj;
					//	String name = (String)jobj.get("name");
						String id = (String)jobj.get("id");
				//		System.out.println("id is  "+ id);
//					System.out.println(temp.get("name")+ ", "+temp.get("id"));
//						System.out.println(topo.get(i).getClass().getName());
				//		topologies.add(new Topology((String)jobj.get("id")));
						topologies.put(id, new Topology(id));
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	//		System.out.println("values " +topologies.toString());
			conn.disconnect();
		}


	// get the active topology info
	public void Topologyinfo(){
		if (topologies.size() == 0)
			System.out.println("no topology is working at the moment");
		else{
			for(Entry<String, Topology> e : topologies.entrySet()){
			//	System.out.println("key is "+e.getKey());
				topologyworker(e.getKey());
			}
		
			try {
				File f = new File(Constants.topologyworker);
				FileWriter fw = new FileWriter(f);
				for(Entry<String, Topology> e : topologies.entrySet()){
					String time = Methods.formattime();
					fw.write(time +" , "+ e.getKey() + " , "+ e.getValue().tworker.toString()+"\n");
					fw.flush();
				}
				fw.close();
			}catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				}
		}
	}
	
	
	// get the worker information of each topology
	public void topologyworker(String id){
		Connect("/api/v1/topology-workers/"+id);
		try{
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				obj = parser.parse(output);
				jobj = (JSONObject)obj;
				
				// check the type of return value
			//	System.out.println(jobj.get("hostPortList").getClass().getName());
			    JSONArray topo = (JSONArray) jobj.get("hostPortList");
				for (int i = 0 ; i< topo.size(); i++){
					obj = topo.get(i);
					jobj = (JSONObject) obj;
					topologies.get(id).getTworker().put((String)jobj.get("host"), (Long)jobj.get("port"));
//							topologies.put((String)jobj.get("name"), (String)jobj.get("id"));
					
					}
			}
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
	
	// get the active supervisor information 
	public void Supervisorinfo(){
		    workers = new HashMap<String, Supervisor>();
				Connect("/api/v1/supervisor/summary");
				try {
					while((output = br.readLine()) != null){
						JSONParser parser = new JSONParser();
					//	System.out.println(output);
						obj = parser.parse(output);
						jobj = (JSONObject)obj;
						JSONArray topo = (JSONArray) jobj.get("supervisors");
//						System.out.println("size is "+topo.size());
						for (int i = 0 ; i< topo.size(); i++){
							Object tobj = topo.get(i);
							JSONObject tjobj = (JSONObject) tobj;
					//		System.out.println(tjobj.get("id"));
							String host = (String)tjobj.get("host");
							Supervisor s = new Supervisor((String)tjobj.get("id"),(Long)tjobj.get("slotsTotal"),
							(Long)tjobj.get("slotsUsed"),(Double)tjobj.get("totalMem"),(Double)tjobj.get("totalCpu"),
							(Double)tjobj.get("usedMem"),(Double)tjobj.get("usedCpu"));
//						workers.add(new Supervisor((String)tjobj.get("id"),(String)tjobj.get("host"),(Long)tjobj.get("slotsTotal"),
//								(Long)tjobj.get("slotsUsed"),(Double)tjobj.get("totalMem"),(Double)tjobj.get("totalCpu"),
//								(Double)tjobj.get("usedMem"),(Double)tjobj.get("usedCpu")));
							workers.put(host, s);
						}
						
						//System.out.println(output+ "\n");
						}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			
////			System.out.println(System.nanoTime());
//			System.out.println("workers status now");
//			for(Entry<String, Supervisor> s : workers.entrySet())
//				System.out.println(s.getValue().toString());
			
			conn.disconnect();
		
	}
	
	public void topologySum(){
		for(Entry<String, Topology> e : topologies.entrySet()){
				topologySpoutInitial(e.getKey());
				topologyBoltInitial(e.getKey());
			}
		
	}
	
	//get the spout and bolt of each topology
	public void topologySpoutInitial(String id){
		Connect("/api/v1/topology/"+id);
		try{
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				obj = parser.parse(output);
				jobj = (JSONObject)obj;	
		
				// check the type of return value
//				System.out.println(jobj.getClass().getName());
			    JSONArray topo = (JSONArray)jobj.get("spouts");
			  
			    // only allow one spout for each topology
			    for (int i = 0; i<topo.size() ; i++){
//			    System.out.println("get output "+topo);
			   
			    	obj = topo.get(0);
			    	jobj = (JSONObject) obj;
//					topologies.get(id).getTworker().put((String)jobj.get("host"), (Long)jobj.get("port"));
			//		System.out.println("latency is "+(String)jobj.get("completeLatency"));
//							topologies.put((String)jobj.get("name"), (String)jobj.get("id"));
			    	String spoutid = (String)jobj.get("spoutId");
			    	topologies.get(id).setSpout(new Spout(spoutid));
			    }
			}
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
	}
	
	public void topologyBoltInitial(String id){
		Connect("/api/v1/topology/"+id);
		try{
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				obj = parser.parse(output);
				jobj = (JSONObject)obj;	
		
				// check the type of return value
//				System.out.println(jobj.getClass().getName());
				JSONArray temp = (JSONArray)jobj.get("bolts");
				ArrayList<Bolt> b = new ArrayList<Bolt>();
				for (int i = 0 ; i< temp.size(); i++){
					obj = temp.get(i);
					jobj = (JSONObject) obj;
					String boltid = (String)jobj.get("boltId");
					b.add(new Bolt(boltid));
//					if (i == 0){
//						parallel = String.valueOf(jobj.get("executors"));
//					}
			   }
				topologies.get(id).setBolts(b);			
			}
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.disconnect();
	}
	
	
	

}

