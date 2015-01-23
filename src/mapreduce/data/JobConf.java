/**
 * 
 */
package mapreduce.data;

import java.io.Serializable;

/**
 * Represents the information needed to process a Map Reduce job.
 * @author surajd
 *
 */
@SuppressWarnings("rawtypes")
public class JobConf implements Serializable{
	
	private static final long serialVersionUID = -4862861366583143607L;
	
	private Class mapper;
	private Class reducer;
	
	private Class mapInputKey;
	private Class mapInputValue;
	
	private Class reduceInputKey;
	private Class reduceInputValue;
	
	private Class reduceOutputKey;
	private Class reduceOutputValue;
	
	private String[] inputfilePaths;
	private String outputDirPath;
	
	private Class inputFormat;
	
	private int numberOfReducers;
		
	private String jarPath;
	private String jarName;
	
	/**
	 * @return the numberOfReducers
	 */
	public int getNumberOfReducers() {
		return numberOfReducers;
	}
	/**
	 * @param numberOfReducers the numberOfReducers to set
	 */
	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
	
	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputDirPath;
	}
	/**
	 * @param outputPath the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputDirPath = outputPath;
	}
	/**
	 * @return the mapper
	 */
	public Class getMapper() {
		return mapper;
	}
	/**
	 * @param mapper the mapper to set
	 */
	public void setMapper(Class mapper) {
		this.mapper = mapper;
	}
	/**
	 * @return the reducer
	 */
	public Class getReducer() {
		return reducer;
	}
	/**
	 * @param reducer the reducer to set
	 */
	public void setReducer(Class reducer) {
		this.reducer = reducer;
	}
	/**
	 * @return the mapInputKey
	 */
	public Class getMapInputKey() {
		return mapInputKey;
	}
	/**
	 * @param mapInputKey the mapInputKey to set
	 */
	public void setMapInputKey(Class mapInputKey) {
		this.mapInputKey = mapInputKey;
	}
	/**
	 * @return the mapInputValue
	 */
	public Class getMapInputValue() {
		return mapInputValue;
	}
	/**
	 * @param mapInputValue the mapInputValue to set
	 */
	public void setMapInputValue(Class mapInputValue) {
		this.mapInputValue = mapInputValue;
	}
	/**
	 * @return the reduceOutputKey
	 */
	public Class getReduceOutputKey() {
		return reduceOutputKey;
	}
	/**
	 * @param reduceOutputKey the reduceOutputKey to set
	 */
	public void setReduceOutputKey(Class reduceOutputKey) {
		this.reduceOutputKey = reduceOutputKey;
	}
	/**
	 * @return the reduceOutputValue
	 */
	public Class getReduceOutputValue() {
		return reduceOutputValue;
	}
	/**
	 * @param reduceOutputValue the reduceOutputValue to set
	 */
	public void setReduceOutputValue(Class reduceOutputValue) {
		this.reduceOutputValue = reduceOutputValue;
	}
	/**
	 * @return the inputPaths
	 */
	public String[] getInputPaths() {
		return inputfilePaths;
	}
	/**
	 * @param inputPaths the inputPaths to set
	 */
	public void setInputPaths(String[] inputPaths) {
		this.inputfilePaths = inputPaths;
	}
	/**
	 * @return the inputFormat
	 */
	public Class getInputFormat() {
		return inputFormat;
	}
	/**
	 * @param inputFormat the inputFormat to set
	 */
	public void setInputFormat(Class inputFormat) {
		this.inputFormat = inputFormat;
	}

	
	/**
	 * @return the jarPath
	 */
	public String getJarPath() {
		return jarPath;
	}
	/**
	 * @param jarPath the jarPath to set
	 */
	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}
	/**
	 * @return the jarName
	 */
	public String getJarName() {
		return jarName;
	}
	/**
	 * @param jarName the jarName to set
	 */
	public void setJarName(String jarName) {
		this.jarName = jarName;
	}
	/**
	 * @return the reduceInputKey
	 */
	public Class getReduceInputKey() {
		return reduceInputKey;
	}
	/**
	 * @param reduceInputKey the reduceInputKey to set
	 */
	public void setReduceInputKey(Class reduceInputKey) {
		this.reduceInputKey = reduceInputKey;
	}
	/**
	 * @return the reduceInputValue
	 */
	public Class getReduceInputValue() {
		return reduceInputValue;
	}
	/**
	 * @param reduceInputValue the reduceInputValue to set
	 */
	public void setReduceInputValue(Class reduceInputValue) {
		this.reduceInputValue = reduceInputValue;
	}
	
	

}
