/**
 * 
 */
package mapreduce.data;


/**
 * Stores necessary information about the tasks running on each slave node.
 * @author surajd
 *
 */
//TODO do I need this class at all?

public class SlaveNodeInfo {
	
	public enum SlaveNodeState
	{
		ALIVE,
		DEAD;
	}
	
	// state of this worker.
	private SlaveNodeState nodeState;
	
	// last received timestamp for this host.
	private long lastUpdateReceivedTimestamp;
	
	//current number of map tasks on this slave node.
	private int numberOfRunningMapTasks;
	
	// current number of reducer tasks 
	private int numberOfRunningReduceTasks;
	
	/**
	 * @return the lastUpdateReceivedTimestamp
	 */
	public long getLastUpdateReceivedTimestamp() {
		return lastUpdateReceivedTimestamp;
	}
	/**
	 * @param lastUpdateReceivedTimestamp the lastUpdateReceivedTimestamp to set
	 */
	public void setLastUpdateReceivedTimestamp(long lastUpdateReceivedTimestamp) {
		this.lastUpdateReceivedTimestamp = lastUpdateReceivedTimestamp;
	}
	/**
	 * @return the nodeState
	 */
	public SlaveNodeState getNodeState() {
		return nodeState;
	}
	/**
	 * @param nodeState the nodeState to set
	 */
	public void setNodeState(SlaveNodeState nodeState) {
		this.nodeState = nodeState;
	}
	/**
	 * @return the numberOfRunningMapTasks
	 */
	public int getNumberOfRunningMapTasks() {
		return numberOfRunningMapTasks;
	}
	/**
	 * @param numberOfRunningMapTasks the numberOfRunningMapTasks to set
	 */
	public void setNumberOfRunningMapTasks(int numberOfRunningMapTasks) {
		this.numberOfRunningMapTasks = numberOfRunningMapTasks;
	}
	/**
	 * @return the numberOfRunningReduceTasks
	 */
	public int getNumberOfRunningReduceTasks() {
		return numberOfRunningReduceTasks;
	}
	/**
	 * @param numberOfRunningReduceTasks the numberOfRunningReduceTasks to set
	 */
	public void setNumberOfRunningReduceTasks(int numberOfRunningReduceTasks) {
		this.numberOfRunningReduceTasks = numberOfRunningReduceTasks;
	}
	
	public void incrementNumOfMapTasks()
	{
		this.numberOfRunningMapTasks++;
	}
	
	public void decrementNumOfMapTasks()
	{
		this.numberOfRunningMapTasks--;
	}
	
	public void incrementNumOfReduceTasks()
	{
		this.numberOfRunningReduceTasks++;
	}
	
	public void decrementNumOfReduceTasks()
	{
		this.numberOfRunningReduceTasks--;
	}

}
