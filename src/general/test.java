package general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class test {
	public static void main(String[] args) {
		Random rd = new Random();
		int max = 5;
		for(int i= 0 ;i <5;i++){
			System.out.println(rd.nextInt(5));
		}
	}
	public boolean feedingUpdate(String filename){
		System.out.println("insdie feeing update");
		boolean reschedule = true;
		File file = new File(filename);
		FileReader reader = null;
		BufferedReader br = null;
		String line;
		String name = null;
		HashMap<String, ArrayList<String>> mapping = new HashMap<>();
		ArrayList<String> temp;
		try{
			System.out.println("stargint try");
			reader = new FileReader(file);
			System.out.println("read file");
			br = new BufferedReader(reader);
//			TopologyScheduler ts = null;
			if(br.readLine() == null){
				System.out.println("file is null");
				reschedule = false;
			}
			System.out.println("start reading");
			while((line=br.readLine())!=null){
				System.out.println("line is "+line);
				//the file format should be 1 [s1,m1]
				String pri = line.substring(0, 1);
				int indl = line.indexOf('[');
				int indr = line.indexOf(']');
				String[] t = line.substring(indl+1, indr).split(",");
				temp = new ArrayList<String>();
				for(String s: t){
					temp.add(s);
				}
				mapping.put(pri, temp);
			}
			System.out.println("fiie null");
		}
		catch(FileNotFoundException e){
			System.out.println("not find the file");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		setList(mapping);
//		setActivenum(convertReading(temp));
		for(String s: mapping.keySet()){
			System.out.print(s);
			for(String ss : mapping.get(s)){
				System.out.print(ss+" ");
			}
			System.out.println();
		}
		return reschedule;
	}
}