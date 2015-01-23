package mapreduce.data;

import mapreduce.interfaces.WritableComparable;


public class Entry<Key extends WritableComparable<Key>  ,Value extends WritableComparable<Value>> 
	implements Comparable<Entry<Key , Value>> {

	
	private Key k;
	private Value v;
	
	public Entry(Key k , Value v)
	{
		this.k = k;
		this.v = v;
	}
	
	public Key getKey()
	{
		return k;
	}
	
	public Value getValue()
	{
		return v;
	}

	@Override
	public int compareTo(Entry<Key, Value> o) {
		return this.getKey().compareTo(o.getKey());
	}

}

