package mapreduce.test;
import java.io.IOException;

import mapreduce.data.LongWritable;
import mapreduce.data.Text;
import mapreduce.impl.MapOutputCollector;
import mapreduce.interfaces.Mapper;


/**
 * @author surajd
 *
 */

public class AdjListMap  implements
		Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, MapOutputCollector<Text, Text> output)
		 {
		String line = value.toString();
		
		// Ignore comments in the dataset
		if (line.startsWith("#")) {
			return;
		}

		String[] splitLine = line.split("\\s+");
		output.collect(new Text(splitLine[0]), new Text(splitLine[1]));
	}
}
