/**
 * 
 */
package mapreduce.test;

import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.data.IntWritable;
import mapreduce.data.LongWritable;
import mapreduce.data.Text;
import mapreduce.dfs.Sfs;
import mapreduce.impl.MapOutputCollector;
import mapreduce.impl.ReduceOutputCollector;
import mapreduce.interfaces.Mapper;
import mapreduce.interfaces.Reducer;
import mapreduce.master.MasterNode;

/**
 * Example application to test the map reduce framework.
 * @author surajd
 *
 */
public class WordCount {
	
	public static class Map implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final IntWritable ONE = new IntWritable(1);

		@Override
		public void map(LongWritable keyIn, Text valueIn, MapOutputCollector<Text, IntWritable> collector)
		{
			StringTokenizer tokenizer = new StringTokenizer(valueIn.getData(), " ");
			while(tokenizer.hasMoreTokens())
			{
				Text thisText = new Text(tokenizer.nextToken());
				collector.collect(thisText, ONE);
			}
		}
		
	}
	
	public static class Reduce implements Reducer<Text, IntWritable, Text, IntWritable>
	{

		@Override
		public void reduce(Text keyIn, Iterator<IntWritable> values, ReduceOutputCollector<Text, IntWritable> outputCollector)
		{
			int sum = 0 ;
			
			while ( values.hasNext())
			{
				sum += values.next().getData();
			}
			
			outputCollector.collect(keyIn, new IntWritable(sum));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception
	{
		Sfs.initialize();

		MasterNode masterNode = new MasterNode("abcd");
		
//		Thread.sleep(5000);
//		
//		
//		System.out.println("Done init");
//		
//		SfsPath dir = Sfs.getPath("sfs://job1/input/");
//		
//		if(!Sfs.exists(dir))
//			Sfs.createDirectories((dir));
//		
//		SfsPath file = Sfs.getPath("sfs://job1/input/4300.txt");
//		
//		Path acutalFile = Paths.get("/Users/surajd/Desktop/4300.txt");
//		
//		Sfs.copy(acutalFile, file);
//		
//		SfsPath dir1 = Sfs.getPath("sfs://job1/output/");
//		
//		if(!Sfs.exists(dir1))
//			Sfs.createDirectories((dir1));
//		
//		
//		JobConf job = new JobConf();
//		
//		job.setMapper(Map.class);
//		job.setReducer(Reduce.class);
//		
//		job.setJarName("WordCount");
//		job.setJarPath("/Users/surajd/Desktop/Word.jar");
//		
//		job.setMapInputKey(LongWritable.class);
//		job.setMapInputValue(Text.class);
//		job.setMapOutputKey(Text.class);
//		
//		
//		
//		job.setReduceOutputKey(String.class);
//		job.setReduceOutputValue(Integer.class);
//		
//		
//		//job.setInputPaths(new String[]{"/Users/surajd/Desktop/4300.txt"});
//		job.setInputPaths(new String[]{"sfs://job1/input/4300.txt"});
//		job.setOutputPath("sfs://job1/output/");
//		
//		job.setInputFormat(TextInputFormat.class);
//		//job.setRecordSize(50);
//		
//		job.setNumberOfMappers(1);
//		job.setNumberOfReducers(1);
//		
//		JobClient.runJob(job);
		
	}

}
