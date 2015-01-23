package mapreduce.input;

import java.util.Iterator;

import mapreduce.data.Text;
import mapreduce.interfaces.WritableComparable;

public class CombinedFileEntry
{
	public Text key;
	public Iterator<WritableComparable<?>> iterator;
	
	public CombinedFileEntry(Text key, Iterator iterator)
	{
		this.key = key;
		this.iterator = iterator;
	}

	/**
	 * @return the key
	 */
	public Text getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(Text key) {
		this.key = key;
	}

	/**
	 * @return the iterator
	 */
	public Iterator<WritableComparable<?>> getIterator() {
		return iterator;
	}

	/**
	 * @param iterator the iterator to set
	 */
	public void setIterator(Iterator<WritableComparable<?>> iterator) {
		this.iterator = iterator;
	}
	
	
	
}