/**
 * 
 */
package mapreduce.impl;

import java.util.Collection;
import java.util.LinkedList;

import mapreduce.data.Entry;
import mapreduce.interfaces.WritableComparable;

/**
 * @author surajd
 *
 */
public class ReduceOutputCollector< Key extends WritableComparable<Key> ,Value extends WritableComparable<Value>> {
	
	// number of partitions needed for the output values.
	Collection<Entry<? extends WritableComparable<?>, ? extends WritableComparable<?>>> reduceOutput;
	
	
	public ReduceOutputCollector() {
		this.reduceOutput = new LinkedList<>();
	}
	
	public void collect(Key k, Value v) {
		reduceOutput.add(new Entry<Key,Value>(k,v));
	}

	public Collection<Entry<? extends WritableComparable<?>, ? extends WritableComparable<?>>> retrieveCollectedValues() {
		return reduceOutput;
	}
	

	
}
