package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.storm.starter.TrendingTopic;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PerformanceUpdate implements Runnable{
//public class PerformanceUpdate {
	HashMap<String, Topology> topologies;
	String hostport;
	URL url;
	HttpURLConnection conn;
	Object obj;
	JSONObject jobj;
	String freq;
	
	public PerformanceUpdate(HashMap<String, Topology> t, String hostport) {
		// TODO Auto-generated constructor stub
		this.topologies = t;
		this.hostport =hostport;
	//	this.freq = freq;
		
	}
//	public void updating(){
//	@Override
	public void run() {
		// TODO Auto-generated method stub
		for(Entry<String, Topology> e : topologies.entrySet()){
			System.out.println("start now ");
			ComponenetsPerf(e.getKey());
//			String f = TrendingTopic.freq;
//			String p = tRE.parallel;
			writeFile(e.getKey());
		}
	}
	
	public void writeFile(String id){
		try {
			String path = Constants.topologysum+".txt";
			File f = new File(path);
			FileWriter fw = new FileWriter(f,true);
			String time = Methods.formattime();
			Topology t = topologies.get(id);
//			fw.write(time + " , "+ t.getSpout().id+ " , "+t.getSpout().getLatency()+" , "+t.getSpout().getTransfered()+"\n");
			fw.write(time+" , "+t.getSpout().toString());
			for (Bolt b : t.getBolts()){	
//				fw.write(b.id+" , "+b.getEmit()+" , "+b.getProcessdelay()+" , "+b.getExecutedelay()+ " , "+"\n");
				fw.write(b.toString());
			}
			fw.flush();
				
			fw.close();
			}
			catch (IOException e1) {
					// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	}
	
	public void ComponenetsPerf(String id){
		try{
			url = new URL(hostport+"/api/v1/topology/"+id);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode() != 200){
				throw new RuntimeException("Failed : http error code"+ conn.getResponseCode());
				}
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String output;
		
			while((output = br.readLine()) != null){
				JSONParser parser = new JSONParser();
				obj = parser.parse(output);
				jobj = (JSONObject)obj;	
		
				// check the type of return value
//				System.out.println(jobj.getClass().getName());
				JSONArray temp = (JSONArray)jobj.get("spouts");
				JSONArray ttemp = (JSONArray)jobj.get("bolts");
				for (int i = 0 ; i< temp.size(); i++){
					obj = temp.get(i);
					jobj = (JSONObject) obj;
					String latency = (String)jobj.get("completeLatency");
					Long transfered= (Long)jobj.get("transferred");
					Long emitted = (Long)jobj.get("emitted");
					int executor = (Integer)jobj.get("executors");
					topologies.get(id).getSpout().setLatency(latency);
					topologies.get(id).getSpout().setTransfered(transfered);
					topologies.get(id).getSpout().setExecutor(executor);
					topologies.get(id).getSpout().setEmitted(emitted);
			   }
				for (int i = 0; i<ttemp.size(); i++){
					obj = ttemp.get(i);
					jobj = (JSONObject) obj;
			//		topologies.get(id).getBolts().get(i).setCapacity((String)jobj.get("capacity"));
					topologies.get(id).getBolts().get(i).setExecutedelay((String)jobj.get("executeLatency"));
					topologies.get(id).getBolts().get(i).setProcessdelay((String)jobj.get("processLatency"));
					topologies.get(id).getBolts().get(i).setEmit((Long)jobj.get("emitted"));
					topologies.get(id).getBolts().get(i).setExecuted((Long)jobj.get("executed"));
					topologies.get(id).getBolts().get(i).setExecutor((Integer)jobj.get("executors"));
					topologies.get(id).getBolts().get(i).setTransferred((Long)jobj.get("transferred"));
				
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
}
