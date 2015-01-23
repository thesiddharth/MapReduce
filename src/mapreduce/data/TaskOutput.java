/**
 * 
 */
package mapreduce.data;

import java.io.Serializable;

/**
 * Represents the output of each map task. 
 * @author surajd
 *
 */
public class TaskOutput implements Serializable {
	
	private static final long serialVersionUID = -6332978844022439666L;
	private String ipAddress;
	private String filePath;
	
	public TaskOutput(String ipAddress , String filePath) {
		super();
		this.ipAddress = ipAddress;
		this.filePath = filePath;
	}

	/**
	 * @return the mapperInfo
	 */
	public String getIpAddress() {
		return ipAddress;
	}

	/**
	 * @param mapperInfo the mapperInfo to set
	 */
	public void setHostIpAddress(String hostIp) {
		this.ipAddress = hostIp;
	}

	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	

}
