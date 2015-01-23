import mapreduce.data.IntWritable;
import mapreduce.data.LongWritable;
import mapreduce.data.Text;
import mapreduce.impl.MapOutputCollector;
import mapreduce.interfaces.Mapper;

import java.util.StringTokenizer;

public class Map implements Mapper<LongWritable, Text, Text, IntWritable>
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