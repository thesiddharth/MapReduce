/**
 * 
 */
package mapreduce.input;

import java.io.IOException;
import java.util.List;

import mapreduce.dfs.data.SfsFileDoesNotExistException;
import mapreduce.interfaces.WritableComparable;

/**
 * Specifies the input format for the Map Reduce Framwework. Each input record is assumed to be of a fixed size. 
 * Implementations of this Input Format also provide functionality to split the input based on the record size.
 * @author surajd
 *
 */
public abstract class InputFormat<K extends WritableComparable , V extends WritableComparable> {
	
	/**
	 * Gets the {@linkplain InputSplits} for this inputFormat.
	 * @return
	 */
	public abstract List<InputSplit> createInputSplits() throws IOException , SfsFileDoesNotExistException;
	
	/**
	 * Returns the record reader for a split, which can read all the records in a split.
	 * @throws SfsFileDoesNotExistException 
	 */
	public abstract RecordReader<K , V> createRecordReader(InputSplit inputSplit) throws IOException, SfsFileDoesNotExistException;

}
