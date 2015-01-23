/**
 * 
 */
package mapreduce.data;

import java.io.IOException;
import java.io.PrintWriter;

import mapreduce.interfaces.WritableComparable;

/**
 * @author surajd
 *
 */
public class Text implements Comparable<Text> , WritableComparable<Text> {

	private String data;
	
	public Text(String data)
	{
		this.data = data;
	}
	
	/**
	 * @return the data
	 */
	public String getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(String data) {
		this.data = data;
	}

	@Override
	public int compareTo(Text that) {
		return data.compareTo(that.data);
	}

	@Override
	public void writeTo(PrintWriter writer) throws IOException {
		writer.write(data);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return data;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Text other = (Text) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}

}
