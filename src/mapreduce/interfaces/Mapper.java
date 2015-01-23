/**
 * 
 */
package mapreduce.interfaces;

import mapreduce.impl.MapOutputCollector;

/**
 * Interface to be implemented for a Map Task
 * @author surajd
 *
 */
public interface Mapper<KeyIn extends WritableComparable<KeyIn> , ValueIn extends WritableComparable<ValueIn> , KeyOut extends WritableComparable<KeyOut>, ValueOut extends WritableComparable<ValueOut>> {
	
	// maps a single key value input pair to an intermediate output.
	void map(KeyIn keyIn , ValueIn valueIn , MapOutputCollector<KeyOut , ValueOut> collector);

}
