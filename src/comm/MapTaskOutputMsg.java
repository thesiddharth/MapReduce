/**
 * 
 */
package comm;

import java.util.Map;


/**
 * Represents a command from the Master Node to a Slave node to start the specified map task.
 * @author surajd
 *
 */
public class MapTaskOutputMsg extends Message {

	private static final long serialVersionUID = 1346365226382858165L;
	
	private int taskId;
	private double progress;
	private Map<Integer , String> outputPaths;
	
	
	public MapTaskOutputMsg(int taskId , double progress)
	{
		this.taskId = taskId;
		this.progress = progress;
	}
	
	
	public MapTaskOutputMsg(int taskId, Map<Integer, String> outputPaths)
	{
		this.taskId = taskId;
		this.outputPaths = outputPaths;
		this.progress = 0.0;
	}
	

	/**
	 * @return the progress
	 */
	public double getProgress() {
		return progress;
	}


	/**
	 * @param progress the progress to set
	 */
	public void setProgress(double progress) {
		this.progress = progress;
	}


	/**
	 * @return the outputPaths
	 */
	public Map<Integer, String> getOutputPaths() {
		return outputPaths;
	}


	/**
	 * @param outputPaths the outputPaths to set
	 */
	public void setOutputPaths(Map<Integer, String> outputPaths) {
		this.outputPaths = outputPaths;
	}

}
