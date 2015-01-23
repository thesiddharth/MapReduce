/**
 * 
 */
package mapreduce.slave;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import mapreduce.util.Util;

import comm.Message;
import comm.Message.MESSAGE_TYPE;

/**
 * A poller thread, to be run on each worker node on the network.
 * Serves as a heartbeat from the worker node to the master.
 * @author surajd
 *
 */
public class WorkerHeartbeat  {
	
	private String masterIpAddress;
	private int masterHeartBeatPort;
	private Socket socket;
	private ScheduledExecutorService executorService;
	
	public WorkerHeartbeat(String masterIpAddress , int heartBeatPort)
	{
		this.masterIpAddress = masterIpAddress;
		this.masterHeartBeatPort = heartBeatPort;
		this.executorService = Executors.newSingleThreadScheduledExecutor();
	}

	public void startPolling()
	{
		executorService.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				try
				{
					socket = new Socket(masterIpAddress , masterHeartBeatPort);
					
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(Util.getHeartBeatMessage());
					
					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					Message message = (Message)inputStream.readObject();
					
					if(! message.getMessageType().equals(MESSAGE_TYPE.ACK) )
					{
						throw new IllegalStateException("Weird reply from master on a heartbeat");
					}
					
					socket.close();
				} 
				catch (IOException | ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, 1, 5, TimeUnit.SECONDS);
	}
	
	

}
