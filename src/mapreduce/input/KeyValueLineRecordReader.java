package mapreduce.input;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Scanner;

import mapreduce.data.Entry;
import mapreduce.data.IntWritable;
import mapreduce.data.Text;

/**
 * Reads each line in a file woth records of fixed size as a key value pair. Is backed by {@linkplain RandomAccessFile} 
 * so we can read chunks efficiently.
 * @author surajd
 *
 */
public class KeyValueLineRecordReader extends RecordReader<Text, IntWritable> {

	private Scanner scanner;

	
	public KeyValueLineRecordReader(String fileName) throws IOException
	{
		this.scanner = new Scanner(new File(fileName));
	}
	
	
	@Override
	public void initialize(InputSplit inputSplit) throws IOException {
		

		
	}
	
	
	@Override
	public boolean hasNextEntry() throws IOException {
		return scanner.hasNext();
		
	}


	@Override
	public Entry<Text, IntWritable> getNextEntry() throws Exception {
		String line = scanner.nextLine();
		Text key = new Text(line.split(" ")[0]);
		IntWritable value = new IntWritable(line.split(" ")[1]);
		return new Entry<Text, IntWritable>(key, value);
	}

	
	
}


