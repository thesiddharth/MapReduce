/**
 * 
 */
package comm;

/**
 * A progress report of a task in progress on one of the nodes.
 * @author surajd
 *
 */
public class TaskStatusMsg extends Message {

	private static final long serialVersionUID = 1967687644021702288L;
	
	private int nodeId;
	private int jobId;
	private int taskId;
	private double progress;
	private Exception exception;
	
	
	public TaskStatusMsg()
	{
		
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
	 * @return the progress
	 */
	public double getProgress() {
		return progress;
	}
	/**
	 * @param progress the progress to set
	 */
	public void setProgress(double progress) {
		this.progress = progress;
	}
	/**
	 * @return the exception
	 */
	public Exception getException() {
		return exception;
	}
	/**
	 * @param exception the exception to set
	 */
	public void setException(Exception exception) {
		this.exception = exception;
	}

	/**
	 * @return the nodeId
	 */
	public int getNodeId() {
		return nodeId;
	}

	/**
	 * @param nodeId the nodeId to set
	 */
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	
	
	

}
