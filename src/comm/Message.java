/**
 * 
 */
package comm;

import java.io.Serializable;

/**
 * Basic unit of communication between partipants in a network
 * @author surajd
 *
 */
public class Message implements Serializable {
	
	private static final long serialVersionUID = 368195576048194539L;

	public static enum MESSAGE_TYPE
	{
		ACK,
		START_NEW_MR_JOB,
		START_MAP_TASK,
		START_REDUCE_TASK,
		MAP_TASK_FINISHED,
		IS_MAP_TASK_FINISHED,
		FAILED, REGISTER_WITH_SERVER,
		MAP_TASK_RUNNING, 
		MAP_TASK_STATUS_MSG, 
		REDUCE_TASK_STATUS_MSG,
		HEARTBEAT_MSG,
	}
	
	// type of this message
	private MESSAGE_TYPE messageType;
	// any generic metadata that may be sent along.
	private Object message;
	
	/**
	 * @return the messageType
	 */
	public MESSAGE_TYPE getMessageType() {
		return messageType;
	}

	/**
	 * @param messageType the messageType to set
	 */
	public void setMessageType(MESSAGE_TYPE messageType) {
		this.messageType = messageType;
	}

	/**
	 * @return the message
	 */
	public Object getMessage() {
		return message;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(Object message) {
		this.message = message;
	}
	
}
