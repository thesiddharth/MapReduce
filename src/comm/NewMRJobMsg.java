package comm;

import mapreduce.data.JobConf;

/**
 * Contains information needed to start a new MR job. Sent from the machine where
 * the job is started to the Master Node.
 * @author surajd
 *
 */
public class NewMRJobMsg extends Message{

	private static final long serialVersionUID = 7134025572991411342L;
	private JobConf jobConf;
	private String clientNodeIp;

	/**
	 * @return the job
	 */
	public JobConf getJobConf() {
		return jobConf;
	}

	/**
	 * @param job the job to set
	 */
	public void setJob(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	/**
	 * @return the clientNodeIp
	 */
	public String getClientNodeIp() {
		return clientNodeIp;
	}

	/**
	 * @param clientNodeIp the clientNodeIp to set
	 */
	public void setClientNodeIp(String clientNodeIp) {
		this.clientNodeIp = clientNodeIp;
	}
	
}
