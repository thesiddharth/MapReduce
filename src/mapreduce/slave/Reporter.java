/**
 * 
 */
package mapreduce.slave;

import java.util.Map;

import mapreduce.data.HostInfo;
import mapreduce.data.MapReduceException;
import comm.CommManager;
import comm.MapTaskStatusMsg;
import comm.Message;
import comm.TaskStatusMsg;
import comm.Message.MESSAGE_TYPE;
import comm.ReduceTaskStatusMsg;

/**
 * Used by a slave node to report progress on a task.
 * @author surajd
 *
 */
public class Reporter {
	
	// the address of the master node on the network.
	private HostInfo masterNodeHostInfo;
	// the slave nodes id.
	private int nodeId;
	// the jobId which the slave is currently working on
	private int jobId;
	// the taskId which the slave is currently working on.
	private int taskId;
	
	
	public Reporter(HostInfo masterNodeHostInfo, int nodeId, int jobId,
			int taskId) {
		super();
		this.masterNodeHostInfo = masterNodeHostInfo;
		this.nodeId = nodeId;
		this.jobId = jobId;
		this.taskId = taskId;
	}
	
	/**
	 * Report progress by sending a message to the master.
	 * @param message
	 */
	public Message  reportMapperProgress(double progress) throws Exception
	{
		return reportMapperProgress(progress , null);
	}
	
	/**
	 * Report progress by sending a message to the master.
	 * @param message
	 */
	public Message reportMapperProgress(double progress, Map<Integer,String> mapOutputPaths) throws Exception
	{
		MapTaskStatusMsg message = new MapTaskStatusMsg();
		message.setMessageType(MESSAGE_TYPE.MAP_TASK_STATUS_MSG);
		message.setJobId(jobId);
		message.setTaskId(taskId);
		message.setNodeId(nodeId);
		message.setProgress(progress);
		message.setMapOutputPaths(mapOutputPaths);

		
		return CommManager.sendMessage(masterNodeHostInfo, message);
		
	}

    /**
     * Report progress by sending a message to the master.
     * @param message
     */
    public Message reportMapperProgress(MapReduceException e)
    {
        MapTaskStatusMsg message = new MapTaskStatusMsg();
        message.setMessageType(MESSAGE_TYPE.MAP_TASK_STATUS_MSG);
        message.setJobId(jobId);
        message.setTaskId(taskId);
        message.setNodeId(nodeId);
        message.setException(e);


        try {
            return CommManager.sendMessage(masterNodeHostInfo, message);
        } catch (Exception e1) {
            e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        //TODO
        return null;

    }
	
	/**
	 * Report progress by sending a message to the master.
	 * @param message
	 */
	public Message reportReducerProgress(double progress , String outputPath)
	{
		ReduceTaskStatusMsg message = new ReduceTaskStatusMsg();
		message.setMessageType(MESSAGE_TYPE.REDUCE_TASK_STATUS_MSG);
		message.setJobId(jobId);
		message.setTaskId(taskId);
		message.setNodeId(nodeId);
		message.setProgress(progress);
		message.setOutputPath(outputPath);
		
		Message reply = null;
		try 
		{
			reply = CommManager.sendMessage(masterNodeHostInfo, message);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		return reply;
	}

    /**
     * Report progress by sending a message to the master.
     * @param message
     */
    public Message reportReducerProgress(Exception e)
    {
        ReduceTaskStatusMsg message = new ReduceTaskStatusMsg();
        message.setMessageType(MESSAGE_TYPE.REDUCE_TASK_STATUS_MSG);
        message.setJobId(jobId);
        message.setTaskId(taskId);
        message.setNodeId(nodeId);
        message.setException(e);

        Message reply = null;
        try
        {
            reply = CommManager.sendMessage(masterNodeHostInfo, message);
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

        return reply;
    }
	
	/**
	 * Reports any exception that might have occured to the master.
	 * @return
	 */
	public Message reportException(MapReduceException e  , MESSAGE_TYPE type)
	{
		TaskStatusMsg message = new TaskStatusMsg();
		message.setJobId(jobId);
		message.setNodeId(nodeId);
		message.setTaskId(taskId);
		message.setException(e);
        message.setMessageType(type);

		
		Message reply = null;
		try 
		{
			reply = CommManager.sendMessage(masterNodeHostInfo, message);
		} 
		catch (Exception e1)
		{
			e.printStackTrace();
		}
		return reply;
		
	}
	

	/**
	 * Report progress by sending a message to the master.
	 * @param message
	 */
	public Message reportReducerProgress(double progress) throws Exception
	{
		return reportReducerProgress(progress, null);
	}
	
	/**
	 * @return the masterNodeHostInfo
	 */
	public HostInfo getMasterNodeHostInfo() {
		return masterNodeHostInfo;
	}
	/**
	 * @param masterNodeHostInfo the masterNodeHostInfo to set
	 */
	public void setMasterNodeHostInfo(HostInfo masterNodeHostInfo) {
		this.masterNodeHostInfo = masterNodeHostInfo;
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
	
	
}
