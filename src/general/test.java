package general;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import javax.print.attribute.HashAttributeSet;

import com.sun.research.ws.wadl.Link;

public class test {
	public static void main(String[] args) {
//		Map<String, Integer> t = new HashMap<>();
//		t.put("ccc", 5);
//		t.put("ddd", 1);
//		t.put("aaa", 3);
//		t.put("bbb", 9);
//		System.out.println(t);
//		
//		Map<String, Integer> tt = new TreeMap<String,Integer>(t);
//		System.out.println(tt);
		LinkedList<Double> a = new LinkedList<>();
		LinkedList<Double> b = new LinkedList<>();
		a.add(1.1);
		a.add(2.2);
//		b.add(1.1);
//		b.add(2.2);
//		b.add(3.3);
		LinkedList<Double> c = new LinkedList<>();
		for(double d:a){
			c.add(d);
		}
		for(int i = 0 ;i <c.size();i++){
			if(i<=b.size()-1){
				double temp = c.get(i);
				temp += b.get(i);
				c.set(i, temp);
			}
			for(int j=0; j<b.size()-c.size();j++){
				c.add(b.get(a.size()+j));
			}
		}
		System.out.println(c);
	}
}
