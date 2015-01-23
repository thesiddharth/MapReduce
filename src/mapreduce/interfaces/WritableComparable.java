/**
 * 
 */
package mapreduce.interfaces;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Contract to be implemented by all instances that need to be written.
 * @author surajd
 * @param <T>
 *
 */
public interface WritableComparable<T> extends Comparable<T>{
	
	/**
	 * Write to the output.
	 * @param output
	 */
	public void writeTo(PrintWriter output) throws IOException;
	
	public String toString();

}
