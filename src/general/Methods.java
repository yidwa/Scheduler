package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;


public class Methods {
	public static String formattime(){
		Instant instant = Instant.now (); // Current date-time in UTC.
		String output = instant.toString ();
		output = instant.toString ().replace ( "T" , " " ).replace( "Z" , "");
		return output;
	}
	
	public static DecimalFormat formatter = new DecimalFormat("#0.00");

	public static void writeFile(String sen, String Filename, boolean append){
			try {
				String path = "/Users/yidwa/Desktop/"+Filename;
//				String path = "/home/ubuntu/metrics/"+Filename;
				File f = new File(path);
				FileWriter fw = new FileWriter(f,append);
				String time = Methods.formattime();
				fw.write(time+"\n"+ sen+"\n");
			
//				System.out.println("write "+ sen);
				fw.flush();
					
				fw.close();
				}
				catch (IOException e1) {
						// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		}
	
//	public static ArrayList<Double> getRecords(String s, Boolean arrival){
//	    ArrayList<Double> rate = new ArrayList<Double>();
//		try{
//			 String path = "/Users/yidwa/Desktop/Records.txt";
//			 File f = new File(path);
//			 FileReader fr = new FileReader(f);
//			 BufferedReader br = new BufferedReader(fr);
//			 String line = "";
//			
//			 while((line = br.readLine())!= null){
//				 if(line.contains(s)&& (line.contains("arrival")&&arrival==true)||(line.contains("service")&&arrival==false)){
//					 int start = line.indexOf('[');
//					 int end = line.indexOf(']');
//					 String temp = line.substring(start+1, end);
//					 System.out.println("temp is "+temp);
//					 String[] t = temp.split(", ");
//					 for(String ss: t){
//						 rate.add(Double.valueOf(ss));
//					 }
//				 }
//			 }
//			 br.close();
//			 fr.close();
//		 }
//		 catch(IOException e){
//			 e.printStackTrace();
//		 }
//		 return rate;
//	}
	
	public static String findHosttype(String s, int change){
		String temp = s;
		ArrayList<String> list = new ArrayList<String>();
		list.add("s");
		list.add("m");
		list.add("l");
		int ind = list.indexOf(s);
		int updatind = ind+change;
		if(updatind>=0 && updatind<=2)
			temp = list.get(updatind);
		return temp;
	}
}