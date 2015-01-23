/**
 * 
 */
package comm;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import mapreduce.data.HostInfo;

/**
 * Util class that abstracts communication between nodes on the network.
 * @author surajd
 *
 */
public class CommManager {
	
	/**
	 * Uses a cached socket and sends a message to the other end.
	 * @param hostInfo
	 * @param message
	 * @return
	 * @throws Exception
	 */
	public static Message sendMessageUsingCachedSocket(HostInfo hostInfo , Message message) throws Exception
	{
		if(hostInfo == null || message == null)
		{
			throw new IllegalArgumentException("Null host information or message.");
		}
			
		try
		{
			hostInfo.getObjectOutputStream().writeObject(message);
			Message reply = (Message) hostInfo.getObjectInputStream().readObject();
			return reply;
		} 
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	
	/**
	 * Sends a message by creating a new socket.
	 * @param hostInfo
	 * @param message
	 * @return
	 * @throws Exception
	 */
	public static Message sendMessage(HostInfo hostInfo , Message message) throws Exception
	{
		if(hostInfo == null || message == null)
		{
			throw new IllegalArgumentException("Null host information or message.");
		}
			
		try
		{
			Socket socket = getSocketFromHostInfo(hostInfo);
			writeMessage(message , socket);
			Message reply = readMessage(socket);
			socket.close();
			return reply;
		} 
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	
	//TODO set timeout on sockets.
	public static Socket getSocketFromHostInfo(HostInfo info) throws Exception
	{
		Socket socket = new Socket(info.getIpAddress() , info.getPort());
		return socket;
	}

	public static void writeMessage(Message message , Socket socket) throws Exception
	{
		ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
		outputStream.writeObject(message);
	}

	public static Message readMessage(Socket socket) throws Exception
	{
		ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
		return (Message)inputStream.readObject();
	}
}
