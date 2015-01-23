/**
 * 
 */
package mapreduce.dfs.CommunicationManager;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import mapreduce.data.HostInfo;
import mapreduce.dfs.data.SfsException;
import mapreduce.dfs.data.SfsMessage;

/**
 * Util class that provides communication capability between SFS nodes
 *
 */
public class CommunicationManager {
	
	public static SfsMessage sendMessage(HostInfo hostInfo , SfsMessage message) throws SfsException
    {
        try {
            Socket socket = getSocketFromHostInfo(hostInfo);

            writeSfsMessage(message, socket);

            SfsMessage reply = readSfsMessage(socket);

            socket.close();

            return reply;
        }
        catch (Exception e) {
            throw new SfsException(e);
        }
    }

	private static Socket getSocketFromHostInfo(HostInfo info) throws Exception
	{
		return new Socket(info.getIpAddress() , info.getPort());
	}

	private static void writeSfsMessage(SfsMessage message, Socket socket) throws Exception
	{
		ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
		outputStream.writeObject(message);
	}

	private static SfsMessage readSfsMessage(Socket socket) throws Exception
	{
		ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
		return (SfsMessage)inputStream.readObject();
	}
}
