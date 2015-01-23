package mapreduce.input;

import java.io.Serializable;

import mapreduce.dfs.SfsPath;

public abstract class InputSplit implements Serializable {
	
	private static final long serialVersionUID = -33980726217108947L;
	
	public abstract String getActualFilePath();
	
	public abstract String getFilePath();
	
	public abstract long getLengthOfSplit();
	
}
