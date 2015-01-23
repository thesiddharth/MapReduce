/**
 * 
 */
package mapreduce.master;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.w3c.dom.traversal.NodeIterator;

import mapreduce.data.TaskOutput;
import mapreduce.data.JobConf;
import mapreduce.data.MapReduceException;
import mapreduce.data.MapTask;
import mapreduce.data.ReduceTask;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.SfsPath;
import mapreduce.input.InputSplit;
import mapreduce.input.TextInputFormat;
import mapreduce.master.JobStatus.JobState;
import mapreduce.util.Util;
import comm.MapTaskStatusMsg;
import comm.ReduceTaskOutputMsg;
import comm.ReduceTaskStatusMsg;

/**
 * Util class that provides a way for {@link MasterNode} to manage various MR jobs.
 * @author surajd
 *
 */
public class JobManager {
	
	private static final String JOB_WORKING_DIRECTORY = "/tmp/mapReduce/";
	private Map<Integer, JobConf> jobIdToJobConf;
	private Map<Integer, JobStatus> jobIdToJobStatus;
	private Integer jobCount;
	
	public JobManager()
	{
		jobIdToJobConf = new ConcurrentHashMap<>();
		jobIdToJobStatus = new ConcurrentHashMap<>();
		jobCount = 0;
	}
	/**
	 * Adds a new job, and returns a jobId to the caller.
	 * @param job
	 * @return
	 */
	public int addJob(JobConf job)
	{		
		int jobId = jobCount++;
		jobIdToJobConf.put(jobId , job);
		jobIdToJobStatus.put(jobId, new JobStatus(JOB_WORKING_DIRECTORY + jobId + "/"));
		
		return jobId;
	}
	
	/**
	 * Removes a job.
	 * @param jobId
	 */
	public void removeJob(int jobId)
	{
		validateJobId(jobId);
		jobIdToJobConf.remove(jobId);
	}
	
	/**
	 * Gets the job configuration for the given jobId.
	 * @param jobId
	 * @return
	 */
	public JobConf getJobConf(int jobId)
	{
		validateJobId(jobId);
		return jobIdToJobConf.get(jobId);
	}
	
	/**
	 * Gets the current status of the job with jobId.
	 * @param jobId
	 * @return
	 */
	public JobStatus getJobStatus(int jobId)
	{
		validateJobId(jobId);
		return jobIdToJobStatus.get(jobId);
	}
	
	/**
	 * Gets the input splits for a map job. Looks up the sfs chunk information for this file,
	 * and uses the input format specified to get a list of {@link InputSplit}
	 * @param jobId
	 * @return
	 * @throws Exception 
	 */
	public List<InputSplit> getInputSplitsForMappers(int jobId) throws Exception
	{
		JobConf jobConf = jobIdToJobConf.get(jobId);
				
		TextInputFormat inputFormat = (TextInputFormat) Util.getInputFormatInstanceFromParameters(jobConf.getInputFormat());
		
		SfsPath sfsPath = Sfs.getPath(jobConf.getInputPaths()[0]);
		inputFormat.setFilePath(sfsPath);	
		
		//create and return input splits.
		return inputFormat.createInputSplits();
	
	}
	
	/**
	 * Updates the status of the jobId with the {@link JobState}
	 * @param jobId
	 * @param state
	 */
	public void updateJobStatus(int jobId, JobState state)
	{
		validateJobId(jobId);
		jobIdToJobStatus.get(jobId).setJobState(state);
	}
	
	/**
	 * Gets the input paths for the reduce phase in a job.
	 * @param jobId
	 * @return
	 */
	public Map<Integer, List<TaskOutput>> getReducerInputPaths(int jobId)
	{
		validateJobId(jobId);
		return jobIdToJobStatus.get(jobId).getMapperOutputFiles();
	}
	
	/**
	 * Helper method to validate a jobId.
	 * @param jobId
	 */
	private void validateJobId(int jobId)
	{
		if(! jobIdToJobStatus.containsKey(jobId))
			throw new IllegalStateException(String.format("Job id %d not found" , jobId));
	}
	
	/**
	 * Adds a map task for this job.
	 * @param jobId
	 * @param thisTask
	 */
	public void addMapTask(int jobId, MapTask thisTask) {
		validateJobId(jobId);
		jobIdToJobStatus.get(jobId).addMapTask(thisTask);
	}
	
	/**
	 * Gets all the map tasks for this job id.
	 */
	public Collection<MapTask> getMapTasks(int jobId)
	{
		validateJobId(jobId);
		return jobIdToJobStatus.get(jobId).getMapTasks();
	}
	
	/**
	 * Updates the status of the job received in the task status message.
	 * @param msg
	 * @throws MapReduceException 
	 */
	public void updateMapTaskStatus(MapTaskStatusMsg msg , String mapperIpAddress) throws MapReduceException {
		int jobId = msg.getJobId();
		validateJobId(jobId);
		jobIdToJobStatus.get(jobId).updateMapTaskStatus(msg , mapperIpAddress);
	}
	
	/**
	 * Add a reduce task to this job's set of tasks.
	 * @param jobId
	 * @param thisTask
	 */
	public void addReduceTask(int jobId, ReduceTask thisTask) {
		validateJobId(jobId);
		jobIdToJobStatus.get(jobId).addReduceTask(thisTask);
	}
	
	/**
	 * Gets the reduce tasks for this job.
	 * @param jobId
	 * @return
	 */
	public Collection<ReduceTask> getReduceTasks(int jobId) {
		validateJobId(jobId);
		return jobIdToJobStatus.get(jobId).getReduceTasks();
	}
	/**
	 * Updates status of a reduce task.
	 * @param msg
	 * @throws MapReduceException 
	 */
	public void updateReduceTaskStatus(ReduceTaskStatusMsg msg) throws MapReduceException {
		int jobId = msg.getJobId();
		validateJobId(jobId);
		jobIdToJobStatus.get(jobId).updateReduceTaskStatus(msg);
		
	}
	
	/**
	 * Gets a task Id for a map task in a job.
	 */
	public int getMapTaskId(int jobId)
	{
		validateJobId(jobId);
		return jobIdToJobStatus.get(jobId).getMapTaskId();
	}
	
	/**
	 * Gets a task Id for a reduce task in a job. 
	 */
	public int getReduceTaskId(int jobId)
	{
		validateJobId(jobId);
		return jobIdToJobStatus.get(jobId).getReduceTaskId();
	}
	
	/**
	 * Cleans up resources on the master node.
	 * @param jobId
	 */
	public void cleanUpJob(int jobId) {
		validateJobId(jobId);
		
		jobIdToJobConf.remove(jobId);
		jobIdToJobStatus.remove(jobId);
	}
	
	/**
	 * Gets the working directory for a job.
	 * @param jobId
	 * @return
	 */
	public String getWorkingDirectoryForJob(int jobId) {
		validateJobId(jobId);
		
		return jobIdToJobStatus.get(jobId).getJobWorkingDirectory();
	}
	public Collection<ReduceTask> getReducerOutputs(int jobId) {

		return jobIdToJobStatus.get(jobId).getReduceTasks();
		
		
	}

		
}
