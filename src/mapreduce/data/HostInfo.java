	/**
 * 
 */
package mapreduce.data;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Container class that holds information describing a host.
 * @author surajd
 *
 */
public class HostInfo implements Serializable {
	
	private static final long serialVersionUID = -7478303017142747055L;
	
	private String ipAddress;
	private int port;
	private int sfsPort;
	private ObjectOutputStream objectOutputStream;
	private ObjectInputStream objectInputStream;
	
	
	/**
	 * @return the objectOutputStream
	 */
	public ObjectOutputStream getObjectOutputStream() {
		return objectOutputStream;
	}

	/**
	 * @param objectOutputStream the objectOutputStream to set
	 */
	public void setObjectOutputStream(ObjectOutputStream objectOutputStream) {
		this.objectOutputStream = objectOutputStream;
	}

	/**
	 * @return the objectInputStream
	 */
	public ObjectInputStream getObjectInputStream() {
		return objectInputStream;
	}

	/**
	 * @param objectInputStream the objectInputStream to set
	 */
	public void setObjectInputStream(ObjectInputStream objectInputStream) {
		this.objectInputStream = objectInputStream;
	}

	public HostInfo()
	{
		
	}
	
	public HostInfo(String ipAddress, int port)
	{
		this.ipAddress = ipAddress;
		this.port = port;
	}
	/**
	 * @return the ipAddress
	 */
	public String getIpAddress() {
		return ipAddress;
	}
	/**
	 * @param ipAddress the ipAddress to set
	 */
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the sfsPort
	 */
	public int getSfsPort() {
		return sfsPort;
	}

	/**
	 * @param sfsPort the sfsPort to set
	 */
	public void setSfsPort(int sfsPort) {
		this.sfsPort = sfsPort;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HostInfo [ipAddress=" + ipAddress + ", port=" + port + "]";
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		HostInfo hostInfo = (HostInfo) o;

		if (port != hostInfo.port)
			return false;
		if (!ipAddress.equals(hostInfo.ipAddress))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = ipAddress.hashCode();
		result = 31 * result + port;
		return result;
	}

}
