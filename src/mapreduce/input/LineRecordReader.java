package mapreduce.input;

import java.io.IOException;
import java.nio.charset.Charset;

import mapreduce.data.Entry;
import mapreduce.data.LongWritable;
import mapreduce.data.Text;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.SfsBufferedReader;
import mapreduce.dfs.SfsPath;
import mapreduce.dfs.data.SfsFileDoesNotExistException;

/**
 * Reads the file line by line.
 * so we can read chunks efficiently.
 * @author surajd
 *
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {

	
	private long start;
	private long length;
	private long numLinesRead;
	
	private SfsBufferedReader fileReader;
	private SfsPath actualPath;
	
	public LineRecordReader(InputSplit inputSplit) throws IOException, SfsFileDoesNotExistException
	{
		initialize(inputSplit);
	}
	
	
	@Override
	public void initialize(InputSplit inputSplit) throws IOException, SfsFileDoesNotExistException {
		
		FileInputSplit fileSplit = (FileInputSplit) inputSplit;
		
		length = fileSplit.getLengthOfSplit();
		numLinesRead = 0;
		
		int cnt = 0;
		
		fileReader = Sfs.newBufferedReader(inputSplit.getFilePath(),Charset.defaultCharset());
		
	}
	
	@Override
	public boolean hasNextEntry() {
		return numLinesRead < length;
		
	}

	@Override
	public Entry<LongWritable, Text> getNextEntry() throws IOException {
		long idx =  start + numLinesRead;
		String value = fileReader.readLine();
		numLinesRead++;
		return new Entry<LongWritable, Text >( new LongWritable(idx), new Text(value) );
	}
	
	public void close() throws IOException
	{
		fileReader.close();
	}
	
}


