package mapreduce.data;

import java.util.Comparator;

public  class KeyComparator implements Comparator<Entry>
{

	@Override
	public int compare(Entry o1, Entry o2) {
		return o1.getKey().compareTo(o2.getKey());
	}

}