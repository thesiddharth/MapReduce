/**
 * 
 */
package mapreduce.clients;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import mapreduce.data.HostInfo;
import mapreduce.data.JobConf;
import mapreduce.data.TaskOutput;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.data.SfsCompactReplicaRequest;
import mapreduce.util.Util;

import comm.CommManager;
import comm.Message.MESSAGE_TYPE;
import comm.NewMRJobMsg;
import comm.ReduceTaskOutputMsg;

/**
 * Manager class that starts off the map reduce engine.
 * @author surajd
 *
 */
public class JobClient {
	
	/**
	 * Starts off a Map Reduce job conforming to the input parameters, by sending a message
	 * to the master node on the network.
	 * @param job
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void runJob(JobConf job) throws Exception
	{
		
		NewMRJobMsg msg = new NewMRJobMsg();
		msg.setJob(job);
		msg.setClientNodeIp(InetAddress.getLocalHost().getHostAddress());
		msg.setMessageType(MESSAGE_TYPE.START_NEW_MR_JOB);

        HostInfo masterNodeHostInfo = new HostInfo(Util.getStringConfigValue(Util.MASTER_IP_ADDRESS_KEY)  ,
                (int)Util.getIntConfigValue(Util.MR_MASTER_LISTENING_PORT_KEY));
		
		Socket socket = CommManager.getSocketFromHostInfo(masterNodeHostInfo);
		CommManager.writeMessage(msg, socket);
		CommManager.readMessage(socket);
		
		
		
	}
	
	
	
	

}
