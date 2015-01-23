/**
 * 
 */
package mapreduce.input;

import mapreduce.dfs.SfsPath;


/**
 * Represents a split of an input file.
 * @author surajd
 *
 */
public class FileInputSplit extends InputSplit {

	private static final long serialVersionUID = -3264654183028745154L;
	private String sfsFilePath; // path of the chunk.
	private String actualFilePath; // path of the parent file.
	private long lengthOfSplit; // length of the chunk.
	
	public FileInputSplit(String filePath , String actualFilePath , long lengthOfSplit)
	{
		this.setFilePath(filePath);
		this.setActualFilePath(actualFilePath);
		this.setLengthOfSplit(lengthOfSplit);
	}
	
	/**
	 * @return the actualFilePath
	 */
	public String getActualFilePath() {
		return actualFilePath;
	}

	/**
	 * @param actualFilePath the actualFilePath to set
	 */
	public void setActualFilePath(String actualFilePath) {
		this.actualFilePath = actualFilePath;
	}

	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return sfsFilePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.sfsFilePath = filePath;
	}

	/**
	 * @return the length
	 */
	public long getLengthOfSplit() {
		return lengthOfSplit;
	}

	/**
	 * @param length the length to set
	 */
	public void setLengthOfSplit(long length) {
		this.lengthOfSplit = length;
	}

//	@Override
//	public void setLengthOfSplit(long length) {
//		this.length = length;
//		
//	}
	

}
