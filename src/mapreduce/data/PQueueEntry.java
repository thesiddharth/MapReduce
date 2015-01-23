/**
 * 
 */
package mapreduce.data;

import mapreduce.interfaces.WritableComparable;

/**
 * An entry in the priority queue.
 * @author surajd
 *
 */
public class PQueueEntry<Key extends WritableComparable<Key>,Value extends WritableComparable<Value>> {
	
	private int idx;
	private Entry<Key,Value> entry;
	
	public PQueueEntry(int idx, Entry<Key, Value> entry) {
		super();
		this.idx = idx;
		this.entry = entry;
	}

	/**
	 * @return the idx
	 */
	public int getIdx() {
		return idx;
	}

	/**
	 * @param idx the idx to set
	 */
	public void setIdx(int idx) {
		this.idx = idx;
	}

	/**
	 * @return the entry
	 */
	public Entry<? extends WritableComparable<?>, ? extends WritableComparable<?>> getEntry() {
		return entry;
	}

	/**
	 * @param entry the entry to set
	 */
	public void setEntry(Entry<Key, Value> entry) {
		this.entry = entry;
	}
	
	public Key getKey()
	{
		return entry.getKey();
	}
	
	public Value getValue()
	{
		return entry.getValue();
	}
	
}
