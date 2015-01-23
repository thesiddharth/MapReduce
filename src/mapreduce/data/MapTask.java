package mapreduce.data;

import java.util.HashMap;
import java.util.Map;

import mapreduce.input.InputSplit;

/**
 * Encapsulates progress of a map task on a slave node.
 * @author surajd
 *
 */
public class MapTask extends Task {

	// output paths of the map task
	private Map<Integer , String> outputPaths;
	// the input split this map task is working on.
	private InputSplit inputSplit;
	
	public MapTask()
	{
		super();
		outputPaths =  new HashMap<>();
	}

	/**
	 * @return the mapOutputPaths
	 */
	public Map<Integer, String> getOutputPaths() {
		return outputPaths;
	}
	/**
	 * @param mapOutputPaths the mapOutputPaths to set
	 */
	public void setOutputPaths(Map<Integer, String> outputPaths) {
		this.outputPaths = outputPaths;
	}

	/**
	 * @return the inputSplit
	 */
	public InputSplit getInputSplit() {
		return inputSplit;
	}

	/**
	 * @param inputSplit the inputSplit to set
	 */
	public void setInputSplit(InputSplit inputSplit) {
		this.inputSplit = inputSplit;
	}
}
