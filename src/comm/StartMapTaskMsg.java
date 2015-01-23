/**
 * 
 */
package comm;

import mapreduce.data.JobConf;
import mapreduce.input.InputSplit;

/**
 * Represents a command from the Master Node to a Slave node to start the specified map task.
 * @author surajd
 *
 */
public class StartMapTaskMsg extends Message {

	private static final long serialVersionUID = -6739358946934561505L;
	
	private int jobId;
	private int taskId;
	private InputSplit inputSplit;
	private JobConf jobConf;
	private String localMasterJobDir;
	
	public StartMapTaskMsg(int jobId , int taskId,  JobConf jobConf , InputSplit inputSplit , String localMasterJobDir)
	{
		this.taskId = taskId;
		this.jobId = jobId;
		this.inputSplit = inputSplit;
		this.jobConf = jobConf;
		this.setLocalMasterJobDir(localMasterJobDir);
		
	}

	/**
	 * @return the inputSplit
	 */
	public InputSplit getInputSplit() {
		return inputSplit;
	}

	/**
	 * @param inputSplit the inputSplit to set
	 */
	public void setInputSplit(InputSplit inputSplit) {
		this.inputSplit = inputSplit;
	}

	/**
	 * @return the jobConf
	 */
	public JobConf getJobConf() {
		return jobConf;
	}

	/**
	 * @param jobConf the jobConf to set
	 */
	public void setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	/**
	 * @return the jobId
	 */
	public int getJobId() {
		return jobId;
	}

	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	/**
	 * @return the taskId
	 */
	public int getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId the taskId to set
	 */
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	/**
	 * @return the localMasterJobDir
	 */
	public String getLocalMasterJobDir() {
		return localMasterJobDir;
	}

	/**
	 * @param localMasterJobDir the localMasterJobDir to set
	 */
	public void setLocalMasterJobDir(String localMasterJobDir) {
		this.localMasterJobDir = localMasterJobDir;
	}

}
