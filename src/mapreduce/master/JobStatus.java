/**
 * 
 */
package mapreduce.master;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.data.TaskOutput;
import mapreduce.data.MapReduceException;
import mapreduce.data.MapTask;
import mapreduce.data.ReduceTask;

import comm.MapTaskStatusMsg;
import comm.ReduceTaskStatusMsg;

/**
 * Represents a status of a currently executing job in the MR framework.
 * @author surajd
 *
 */
public class JobStatus {
	
	public enum JobState
	{
		STARTING,
		MAP_TASKS_RUNNING,
		MAP_TASKS_FINISHED,
		REDUCE_TASKS_RUNNING,
		REDUCE_TASKS_FINISHED,
		FINISHED,
	}
	
	//working directory for this job.
	private String jobWorkingDirectory;
	
	// current state of the job.
	private JobState jobState;
	// mapTasks
	private Map<Integer, MapTask> mapTasks;
	// reduceTasks
	private Map<Integer , ReduceTask> reduceTasks;
	//location of the mapper output files.
	private Map<Integer, List<TaskOutput> > mapperOutputFilesByPartition;
	//map task id
	private int mapTaskId;
	// reduce task id/
	private int reduceTaskId;
	
	public JobStatus(String jobWorkingDirectory)
	{
		this.jobState = JobState.STARTING;
		this.mapTasks = new ConcurrentHashMap<>();
		this.reduceTasks = new ConcurrentHashMap<>();
		this.mapperOutputFilesByPartition = new HashMap<>();
		this.mapTaskId = 0;
		this.reduceTaskId = 0;
		this.jobWorkingDirectory = jobWorkingDirectory;
	}
	
	/**
	 * @return the jobState
	 */
	public JobState getJobState() {
		return jobState;
	}
	/**
	 * @param jobState the jobState to set
	 */
	public void setJobState(JobState jobState) {
		this.jobState = jobState;
	}

	/**
	 * @return the mapperOutputFiles
	 */
	public Map<Integer, List<TaskOutput>> getMapperOutputFiles() {
		return mapperOutputFilesByPartition;
	}

	/**
	 * @param mapperOutputFiles the mapperOutputFiles to set
	 */
	public void setMapperOutputFiles(Map<Integer, String> mapperOutputFiles, String mapperIpAddress) {
		
		for(Integer key : mapperOutputFiles.keySet())
		{
			String outputFile = mapperOutputFiles.get(key);
			
			if(mapperOutputFilesByPartition.containsKey(key))
			{
				List<TaskOutput> outputFiles = mapperOutputFilesByPartition.get(key);
				outputFiles.add(new TaskOutput(mapperIpAddress, outputFile));
				mapperOutputFilesByPartition.put(key, outputFiles);
			}
			else
			{
				List<TaskOutput> outputFiles = new LinkedList<>();
				outputFiles.add(new TaskOutput(mapperIpAddress, outputFile));
				mapperOutputFilesByPartition.put(key, outputFiles);
			}
			
		}
		
	}
		
	/**
	 * Adds a job task to track.
	 * @param thisTask
	 */
	public void addMapTask(MapTask thisTask) {
		mapTasks.put(thisTask.getTaskId() , thisTask);
	}
	
	/**
	 * Gets the map tasks for this job.
	 * @return
	 */
	public Collection<MapTask> getMapTasks() {
		return mapTasks.values();
	}
	
	/**
	 * Updates a map tasks status.
	 */
	public void updateMapTaskStatus(MapTaskStatusMsg msg , String mapperIpAddress) throws MapReduceException
	{
		
		// get this task.
		MapTask task = mapTasks.get(msg.getTaskId());
	
//		
//		// double check if the message is valid.
//		if(task.getNodeId() != msg.getNodeId())
//			throw new MapReduceException(new IllegalStateException("Node ids in update map message and local store do not match"));
//		
		task.setException(msg.getException());
		//update task state.
		task.setProgress(msg.getProgress());
		//update last received timestamp.
		task.setLastUpdateReceivedTime(System.currentTimeMillis());
		if( task.getProgress() == 1.0)
		{
			// if the task has finished.
			task.setOutputPaths(msg.getMapOutputPaths());
			setMapperOutputFiles(task.getOutputPaths() , mapperIpAddress);
		}
		
	}
	
	/**
	 * Updates a reduce tasks status.
	 * @throws MapReduceException 
	 */
	public void updateReduceTaskStatus(ReduceTaskStatusMsg msg) throws MapReduceException
	{
		// get this task.
		ReduceTask task = reduceTasks.get(msg.getTaskId());

//		// double check if the message is valid.
//		if (task.getNodeId() != msg.getNodeId())
//			throw new MapReduceException(new IllegalStateException("Node ids in update reduce message and local store do not match"));
		// update the progress made in this task.
		task.setProgress(msg.getProgress());
		// update the latest timestamp.
		task.setLastUpdateReceivedTime(System.currentTimeMillis());
		// update any exception,
		task.setException(msg.getException());
		if (task.getProgress() == 1.0) 
		{
			task.setOutputPaths(msg.getOutputPath());
		}
		
	}

	/**
	 * Adds a reduce task to this job.
	 * @param thisTask
	 */
	public void addReduceTask(ReduceTask thisTask) {
		reduceTasks.put(thisTask.getTaskId() ,thisTask);
	}
	
	/**
	 * Gets the reduce tasks for this job.
	 * @return
	 */
	public Collection<ReduceTask> getReduceTasks() {
		return reduceTasks.values();
	}
	
	/**
	 *  Get a taskId for this map job.
	 */
	public int getMapTaskId()
	{
		return ++mapTaskId;
	}
	
	/**
	 *  Get a taskId for this reduce job job.
	 */
	public int getReduceTaskId()
	{
		return ++reduceTaskId;
	}

	/**
	 * @return the jobWorkingDirectory
	 */
	public String getJobWorkingDirectory() {
		return jobWorkingDirectory;
	}

	/**
	 * @param jobWorkingDirectory the jobWorkingDirectory to set
	 */
	public void setJobWorkingDirectory(String jobWorkingDirectory) {
		this.jobWorkingDirectory = jobWorkingDirectory;
	}
		
}
