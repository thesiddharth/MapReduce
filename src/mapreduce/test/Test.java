package mapreduce.test;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;




public class Test {
	
	private static final String REDUCER_OUTPUT_FILE_PREFIX = "/Users/surajd/Desktop/reducer_output/";
	public static void main(String[] args) throws Exception
	{	
		//JarFile jar = new JarFile(REDUCER_OUTPUT_FILE_PREFIX);
		
//		String localJarPath = "/tmp/mapReduce/0/WC.jar";
//		JarFile jar = new JarFile(localJarPath);
//		
//		URL[] urls = { new URL("jar:file:" + localJarPath+"!/") };
//		URLClassLoader classLoader = new URLClassLoader(urls);
//		
//		Enumeration e = jar.entries();
//		
//		while(e.hasMoreElements())
//		{
//			JarEntry entry = (JarEntry) e.nextElement();
//			//if(entry.g)
//			System.out.println(entry.getName());
//		}
//			
//		Class class1 = classLoader.loadClass("Map");
//		
//		
//		Object obj = class1.newInstance();
//		
//		String dir = "/tmp/0/";
//		new File(dir).mkdirs();
//		
//		FileOutputStream outputStream = new FileOutputStream(new File(dir + "0.txt"));
//		
//		outputStream.write(33);
//		
//		outputStream.close();
		
		final Map<String , Integer> map = new HashMap<>();
		map.put("abcd" , 1);
		map.put("cdef", 2);
		
		List<String> maps = new LinkedList<>();
		maps.add("abcd");
		maps.add("cdef");
		
		Collections.sort(maps, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return -1 * map.get(o1).compareTo(map.get(o2));
			}
		
		});
		
		for(String ma1p : maps)
			System.out.println(ma1p);
		

	}
}
 

	
