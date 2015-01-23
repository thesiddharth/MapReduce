package mapreduce.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Test1 {
	
	public static void main(String[] args) throws Exception
	{
		
		Map<String , Integer > count = new HashMap<>();
		String path1 = "/Users/surajd/Desktop/4300.txt";
		String path2 = "/Users/surajd/Desktop/reducer_output_8/new.txt";
		BufferedReader reader = new BufferedReader(new FileReader(path1));
		
		String input;
		while( (input = reader.readLine())  != null )
		{
			if(input.equals(""))
				continue;
			String[] words = input.split("\\s+");
			
			for(String word : words)
			{
				if(word.equals(""))
					continue;
				if(count.containsKey(word))
				{
					int curCnt = count.get(word);
					curCnt++;
					count.put(word, curCnt);
				}
				else
				{
					count.put(word, 1);
				}
			}
			
		}
		
		reader = new BufferedReader(new FileReader(path2));
		
		
		Set<String> countedWords = new HashSet<>();
		while( (input = reader.readLine()) != null )
		{
			String[] words = input.split(" ");
			
			if(words.length != 2)
			{
				System.out.println("reading weird line " + words);
			}
			else
			{
				int cn1 = count.get(words[0]);
				
				if(count.get(words[0]) !=  Integer.parseInt(words[1]))
				{
					System.out.println("Counts not matching here for word " + words[0] + " Correct cnt " + count.get(words[0]) 
							+ ", incorrect count " + words[1] );
				}
				countedWords.add(words[0]);				
			}
			
		}
		
		if(countedWords.size()  != count.size())
		{
			System.out.println("Incorrect number of words read");
			int cnt = 0;
			for(String word : count.keySet())
			{
				if(!countedWords.contains(word))
				{
					System.out.println(cnt + "##" + word);
					cnt++;
				}
			}
		}
		
		System.out.println("All done");
		
	}
	
}
