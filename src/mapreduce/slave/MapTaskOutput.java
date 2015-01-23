package mapreduce.slave;

/**
 * Container class that represents the progress made in a map task on a slave node.
 * @author surajd
 *
 */
public class MapTaskOutput {

	private String[] outputPaths;
	
	public MapTaskOutput(String[] outputPaths)
	{
		this.outputPaths = outputPaths;
	}

	/**
	 * @return the outputPaths
	 */
	public String[] getOutputPaths() {
		return outputPaths;
	}

	/**
	 * @param outputPaths the outputPaths to set
	 */
	public void setOutputPaths(String[] outputPaths) {
		this.outputPaths = outputPaths;
	}
	
	
	
}
