package mapreduce.input;

import java.io.IOException;

import mapreduce.data.Entry;
import mapreduce.dfs.data.SfsFileDoesNotExistException;
import mapreduce.interfaces.WritableComparable;

/**
 * Record reader breaks down the input into key value pairs for use by a mapper.
 * @author surajd
 *
 * @param <K>
 * @param <V>
 */
public abstract class RecordReader<K extends WritableComparable  , V  extends WritableComparable > {
	
	public abstract void initialize(InputSplit inputSplit) throws IOException , SfsFileDoesNotExistException;
	
	public abstract boolean hasNextEntry() throws Exception;
	
	public abstract Entry<? extends WritableComparable, ? extends WritableComparable> getNextEntry() throws Exception;
		
}
