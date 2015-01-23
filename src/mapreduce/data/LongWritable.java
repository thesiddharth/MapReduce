/**
 * 
 */
package mapreduce.data;

import java.io.IOException;
import java.io.PrintWriter;

import mapreduce.interfaces.WritableComparable;

/**
 * Wrapper class to represent a Long input in the MR framework.
 * @author surajd
 *
 */
public class LongWritable implements Comparable<LongWritable>, WritableComparable<LongWritable> {
	
	private Long data;
	
	public LongWritable(Long data) {
		this.data = data;
	}
	
	public LongWritable(String data)
	{
		this.data = Long.valueOf(data);
	}

	/**
	 * @return the data
	 */
	public Long getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(Long data) {
		this.data = data;
	}

	@Override
	public int compareTo(LongWritable that) {
		return this.data.compareTo(that.data);
	}

	@Override
	public void writeTo(PrintWriter writer) throws IOException {
		writer.print(data);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.valueOf(data);
	}
	

}
