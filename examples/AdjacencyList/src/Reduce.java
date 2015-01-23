import java.util.Iterator;
import mapreduce.data.Text;
import mapreduce.impl.ReduceOutputCollector;
import mapreduce.interfaces.Reducer;
/**
 * 
 */

/**
 * @author surajd
 *
 */
public class Reduce implements Reducer<Text, Text, Text, Text> {
		
	public void reduce(Text key, Iterator<Text> values, ReduceOutputCollector<Text, Text> output) {
			StringBuilder builder = new StringBuilder();
			
			while (values.hasNext()) {
				builder.append(values.next().toString());
				builder.append(",");
			}
			output.collect(key, new Text(builder.toString()));
		}
	}
