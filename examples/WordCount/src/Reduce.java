import mapreduce.data.IntWritable;
import mapreduce.data.Text;
import mapreduce.impl.ReduceOutputCollector;
import mapreduce.interfaces.Reducer;

import java.util.Iterator;

public class Reduce implements Reducer<Text, IntWritable, Text, IntWritable>
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