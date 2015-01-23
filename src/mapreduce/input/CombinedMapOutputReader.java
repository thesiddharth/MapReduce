package mapreduce.input;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import mapreduce.data.IntWritable;
import mapreduce.data.Text;
import mapreduce.interfaces.WritableComparable;

/**
 * Reads each line in a file woth records of fixed size as a key value pair. Is backed by {@linkplain RandomAccessFile} 
 * so we can read chunks efficiently.
 * @author surajd
 *
 */
public class CombinedMapOutputReader  {

	private Scanner scanner;
	private Class valueClass;
	
	
	public CombinedMapOutputReader(String fileName , Class valueClass) throws IOException
	{
		this.scanner = new Scanner(new File(fileName));
		this.valueClass = valueClass;
	}
	


	public boolean hasNextEntry() throws IOException {
		return scanner.hasNext();
		
	}


	public CombinedFileEntry getNextEntry() throws Exception {
		String line = scanner.nextLine();
		Text key = new Text(line.split(" ")[0]);
		String[] values = line.split(" ")[1].split(",");
		List<WritableComparable<?>> writes = new ArrayList<WritableComparable<?>>();
		
		if(valueClass.equals(IntWritable.class))
		{
			for(String value : values)
				writes.add(new IntWritable(value));			
		}
		else if(valueClass.equals(Text.class))
		{
			for(String value : values)
			{
				writes.add(new Text(value));
			}
		}
		
		CombinedFileEntry entry = new CombinedFileEntry(key, writes.iterator());
		return entry;
	}

	
	
}
