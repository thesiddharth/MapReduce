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
public class IntWritable implements Comparable<IntWritable>, WritableComparable<IntWritable> {
	
	private Integer data;
	
	public IntWritable(Integer data) {
		this.data = data;
	}
	
	
	public IntWritable(String data)
	{
		this.data = Integer.valueOf(data);
	}

	/**
	 * @return the data
	 */
	public Integer getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(Integer data) {
		this.data = data;
	}

	@Override
	public int compareTo(IntWritable that) {
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
