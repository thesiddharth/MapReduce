package mapreduce.data;

public class CombineTaskResult {
	private String outputFilePath;
	private int numberOfRecords;
	

	/**
	 * @return the outputFilePath
	 */
	public String getOutputFilePath() {
		return outputFilePath;
	}

	/**
	 * @param outputFilePath the outputFilePath to set
	 */
	public void setOutputFilePath(String outputFilePath) {
		this.outputFilePath = outputFilePath;
	}

	/**
	 * @return the numberOfRecords
	 */
	public int getNumberOfRecords() {
		return numberOfRecords;
	}

	/**
	 * @param numberOfRecords the numberOfRecords to set
	 */
	public void setNumberOfRecords(int numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}
	
	
}
