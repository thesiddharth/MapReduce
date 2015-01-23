/**
 * 
 */
package comm;


/**
 * A progress report of a task in progress on one of the nodes.
 * @author surajd
 *
 */
public class ReduceTaskStatusMsg extends TaskStatusMsg {

	private static final long serialVersionUID = -4161406387526608855L;
	private String outputPath;
	
	public ReduceTaskStatusMsg()
	{
		
	}

	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param outputPath the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	
	

}
