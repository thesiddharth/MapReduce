package mapreduce.data;

import java.util.List;


/**
 * Encapsulates progress of a reduce task on a slave node.
 * @author surajd
 *
 */
public class ReduceTask extends Task {
	
	private List<TaskOutput> inputPaths;
	private String outputPath;
	
	public ReduceTask()
	{
		super();
	}

	/**
	 * @return the mapOutputPaths
	 */
	public String getOutputPath() {
		return outputPath;
	}
	/**
	 * @param mapOutputPaths the mapOutputPaths to set
	 */
	public void setOutputPaths(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @return the inputPaths
	 */
	public List<TaskOutput> getInputPaths() {
		return inputPaths;
	}

	/**
	 * @param filesInThisPartition the inputPaths to set
	 */
	public void setInputPaths(List<TaskOutput> filesInThisPartition) {
		this.inputPaths = filesInThisPartition;
	}
	
	

}
