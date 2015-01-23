/**
 * 
 */
package mapreduce.input;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import mapreduce.data.LongWritable;
import mapreduce.data.Text;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.SfsFileInfo;
import mapreduce.dfs.SfsPath;
import mapreduce.dfs.SfsSegmentInfo;
import mapreduce.dfs.data.SfsFileDoesNotExistException;

/**
 * Input format specifications for reading a text file line by line.
 * @author surajd
 *
 */
public class TextInputFormat extends InputFormat<LongWritable, Text>{

	private SfsPath sfsFilePath;
	
	public TextInputFormat()
	{
		
	}
	
	public TextInputFormat(String filePath) 
	{
		//this.sfsFilePath = Sfs;
		this.sfsFilePath = Sfs.getPath(filePath);
	}
	
	@Override
	public List<InputSplit> createInputSplits() throws IOException, SfsFileDoesNotExistException {
		System.out.println("Creating input spluts for file " + sfsFilePath);
		
		List<InputSplit> splits = new LinkedList<>();
		
		SfsFileInfo fileInfo = Sfs.getFileInfo(sfsFilePath);
		
		List<SfsSegmentInfo> segmentInfos = fileInfo.getSegmentPaths();
		
		for(SfsSegmentInfo segmentInfo : segmentInfos)
		{
			FileInputSplit inputSplit = new FileInputSplit(segmentInfo.getSfsPath().toString() , segmentInfo.getFormalFilePath().toString()
					, segmentInfo.getNumRecords());
			splits.add(inputSplit);
		}
		
		return splits;
		
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit)  throws IOException, SfsFileDoesNotExistException
	{
		return new LineRecordReader(inputSplit);
	}

	/**
	 * @return the filePath
	 */
	public SfsPath getFilePath() {
		return sfsFilePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(SfsPath filePath) {
		this.sfsFilePath = filePath;
	}

}
