/**
 * 
 */
package mapreduce.interfaces;

import java.util.Iterator;

import mapreduce.impl.ReduceOutputCollector;


/**
 * Interface to be implemented by a Reduce Task.
 * @author surajd
 *
 */
public interface Reducer<KeyIn extends WritableComparable<KeyIn> , ValueIn extends WritableComparable<ValueIn> , KeyOut extends WritableComparable<KeyOut> , ValueOut extends WritableComparable<ValueOut>> {
	
	public void reduce(KeyIn keyIn , Iterator<ValueIn> values , ReduceOutputCollector<KeyOut, ValueOut> outputCollector );
	
}
