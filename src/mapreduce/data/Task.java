package mapreduce.data;


/**
 * Encapsulates progress of a map task on a slave node.
 * @author surajd
 *
 */
public class Task {

	// the node on the network where this task is scheduled.
	private int nodeId;
	// taskId assigned to this task.
	private int taskId;
	// progress made in this task.
	private double progress;
	// the last time at which an update was received on this  task.
	private long lastUpdateReceivedTime;
	// any exception that might have occured.
	private Exception exception;
	
	public Task()
	{
		progress = 0.0;
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

	
	/**
	 * @return the lastUpdateReceivedTime
	 */
	public long getLastUpdateReceivedTime() {
		return lastUpdateReceivedTime;
	}

	/**
	 * @param lastUpdateReceivedTime the lastUpdateReceivedTime to set
	 */
	public void setLastUpdateReceivedTime(long lastUpdateReceivedTime) {
		this.lastUpdateReceivedTime = lastUpdateReceivedTime;
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
	
	

}
