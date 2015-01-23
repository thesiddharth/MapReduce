package comm;

import java.util.ArrayList;
import java.util.List;

import mapreduce.data.TaskOutput;
import mapreduce.data.JobConf;


/**
 * Represents the information in a message to start a reduce task.
 * @author surajd
 *
 */
public class StartReduceTaskMsg extends Message{

	private static final long serialVersionUID = -5629309006336055534L;
	
	private int jobId;
	private int taskId;
	private List<TaskOutput> reducerInputPaths;
	private List<String> reducerInputIps;
	private JobConf jobConf;
	private String masterLocalWorkingDir;
	
	public StartReduceTaskMsg()
	{
		this.reducerInputPaths = new ArrayList<>();
		this.reducerInputIps = new ArrayList<>();
	}
	/**
	 * @return the reducerInputPaths
	 */
	public List<TaskOutput> getReducerInputPaths() {
		return reducerInputPaths;
	}
	/**
	 * @param reducerInputPaths the reducerInputPaths to set
	 */
	public void setReducerInputPaths(List<TaskOutput> reducerInputPaths) {
		this.reducerInputPaths = reducerInputPaths;
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
	 * @return the reducerInputIps
	 */
	public List<String> getReducerInputIps() {
		return reducerInputIps;
	}
	/**
	 * @param reducerInputIps the reducerInputIps to set
	 */
	public void setReducerInputIps(List<String> reducerInputIps) {
		this.reducerInputIps = reducerInputIps;
	}
	/**
	 * @return the masterLocalWorkingDir
	 */
	public String getMasterLocalWorkingDir() {
		return masterLocalWorkingDir;
	}
	/**
	 * @param masterLocalWorkingDir the masterLocalWorkingDir to set
	 */
	public void setMasterLocalWorkingDir(String masterLocalWorkingDir) {
		this.masterLocalWorkingDir = masterLocalWorkingDir;
	}
	
}
