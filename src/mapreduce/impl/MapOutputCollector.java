/**
 * 
 */
package mapreduce.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mapreduce.data.Entry;
import mapreduce.data.KeyComparator;
import mapreduce.interfaces.WritableComparable;
import mapreduce.slave.Reporter;

/**
 * Map Output collector. This is responsible for organising the output of the Map phase.
 * It maintains the key value pairs output by the map task, sorted by the key.
 * @author surajd
 *
 */
public class MapOutputCollector<Key extends WritableComparable<Key> ,Value extends WritableComparable<Value>>  {
		
	
	// number of partitions needed for the output values.
	private int numberOfPartitions;
	private Map< Integer , List<Entry<Key,Value> >> partitions;
	
	
	
	
	public MapOutputCollector(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
		this.partitions = new HashMap<>();
	}
	
	public void collect(Key k, Value v) {
		
		int partitionNumber = getPartitionNumber(k);
		
		if(partitions.containsKey(partitionNumber))
		{
			Entry<Key,Value> newEntry = new Entry<Key,Value>(k , v);
			List< Entry<Key,Value> > curList = partitions.get(partitionNumber);
			curList.add(newEntry);
			partitions.put( partitionNumber , curList);
		}
		else
		{
			Entry<Key,Value> newEntry = new Entry<Key,Value>(k , v);
			KeyComparator comparator = new KeyComparator();
			List<Entry<Key, Value>> set = new LinkedList<>();
			set.add(newEntry);
			
			partitions.put(partitionNumber , set);
		}
		
	}

	public Map<Integer , List<Entry<Key, Value> > > retrieveCollectedValues(Reporter reporter , double progress) throws Exception {
		
		// sort the  partitions.
		KeyComparator comparator = new KeyComparator();
		
		for(Integer key : partitions.keySet())
		{
			List<Entry<Key,Value>> values = partitions.get(key);
			Collections.sort(values, comparator);
			partitions.put(key , values);
			reporter.reportMapperProgress(progress);
		}
		return partitions;
	}

	/**
	 * @return the numberOfPartitions
	 */
	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	/**
	 * @param numberOfPartitions the numberOfPartitions to set
	 */
	public void setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}	
	
	//TODO implement types for keys.
	/**
	 * Returns the index of the partition to which this key maps to. 
	 * @param k
	 * @return
	 */
	private int getPartitionNumber(Key k)
	{
		return ( (k.hashCode() % numberOfPartitions) + numberOfPartitions ) % numberOfPartitions;
	}
	
}




