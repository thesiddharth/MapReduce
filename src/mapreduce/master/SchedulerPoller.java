/**
 * 
 */
package mapreduce.master;

import java.util.concurrent.Callable;

import mapreduce.data.HostInfo;
import mapreduce.util.Util;

import comm.CommManager;
import comm.Message;

/**
 * Polls a node after submitting a message to it.
 * @author surajd
 *
 */
public class SchedulerPoller implements Callable<Message> {

	private HostInfo hostInfo;
	private Message message;

	public SchedulerPoller(HostInfo hostInfo , Message message) {
		this.hostInfo = hostInfo;
		this.message = message;
	}
	
	@Override
	public Message call() throws Exception 
	{
		Message reply;
		try
		{
			reply = CommManager.sendMessageUsingCachedSocket(hostInfo, message);
		} 
		catch (Exception e) 
		{
			reply = Util.getFailureMessage();
			e.printStackTrace();
		}
		return reply;
	}
	
}
