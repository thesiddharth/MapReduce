/**
 * 
 */
package mapreduce.master;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import mapreduce.data.TaskOutput;
import mapreduce.data.HostInfo;
import mapreduce.data.JobConf;
import mapreduce.data.MapTask;
import mapreduce.data.ReduceTask;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.CommunicationManager.CommunicationManager;
import mapreduce.dfs.data.SfsCompactReplicaRequest;
import mapreduce.input.InputSplit;
import mapreduce.master.JobStatus.JobState;
import mapreduce.util.ConfigManager;
import mapreduce.util.Util;
import comm.CommManager;
import comm.MapTaskStatusMsg;
import comm.Message;
import comm.Message.MESSAGE_TYPE;
import comm.NewMRJobMsg;
import comm.ReduceTaskOutputMsg;
import comm.ReduceTaskStatusMsg;
import comm.StartMapTaskMsg;
import comm.StartReduceTaskMsg;

/**
 * Encapsulates the responsibilities of the master node in the MR framework.
 * The master is responsible for starting new jobs and maintaining status 
 * information abut running jobs.
 * @author surajd
 *
 */
public class MasterNode {
	
	private static final long MAP_TASK_TIMEOUT = 60 * 1000L;
	protected static final long REDUCE_TASK_TIMEOUT = 60 * 1000L;
	
	private int masterServingPort = 5454;
	
	// tells us how many maps and reduces are running on each host.
	private SlaveNodeManager slaveNodeManager;
	
	// maintanis status of the running jobs.
	private JobManager jobManager;
	
	// master task runner.
	private ExecutorService masterExecutorService;
	
	public MasterNode(String configFilePath) throws Exception
	{
		ConfigManager.init(configFilePath);
		masterServingPort = Util.getIntConfigValue(Util.MR_MASTER_LISTENING_PORT_KEY);
		
		int maxNumberOfRunningJobs = Util.getIntConfigValue(Util.MAX_NUMBER_OF_MR_JOBS);
		masterExecutorService = Executors.newFixedThreadPool(maxNumberOfRunningJobs);
		
		slaveNodeManager = new SlaveNodeManager();
		jobManager = new JobManager();

		new Thread(new MasterServingThread(masterServingPort)).start();
	}

	/**
	 * Starts a new MapReduce job using the input parameters.
	 * @param message
	 * @throws Exception
	 */
	private ReduceTaskOutputMsg startNewMapReduceJob(NewMRJobMsg message)
	{
		JobConf conf = message.getJobConf();
		
		validateJobConf(conf);
		
		if(conf == null)
			throw new IllegalArgumentException("null job configuration passed");
		
		// job id for this MR job.
		int jobId = jobManager.addJob(conf);
		ReduceTaskOutputMsg reply = null;
		try 
		{
			saveClientJarToLocalDisk(message.getClientNodeIp() , jobManager.getWorkingDirectoryForJob(jobId), conf.getJarName()
					,conf.getJarPath());
			
			doMapTask(jobId);
			
			waitForMapTasksToFinish(jobId);
			
			doReduceTask(jobId);
			
			waitForReduceTasksToFinish(jobId);
			
			reply = getReduceOutputMsg(jobManager.getReducerOutputs(jobId));
			
			System.out.println("The reducer output files are located at the following locations:");
			for(TaskOutput taskOutput : reply.getReduceTaskOutputs())
			{
				System.out.println(taskOutput.getIpAddress() + ":" + taskOutput.getFilePath());
			}
			
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		jobManager.cleanUpJob(jobId);
		
		System.out.println("The mapreduce task is finished");
		
		return reply;
	}
	
	
	private ReduceTaskOutputMsg getReduceOutputMsg(Collection<ReduceTask> reducerOutputs) 
	{
		ReduceTaskOutputMsg msg = new ReduceTaskOutputMsg();
		
		for(ReduceTask reducerOutput : reducerOutputs)
		{
			String nodeIp = slaveNodeManager.getSlaveHostConnectionInfo(reducerOutput.getNodeId()).getIpAddress();
			String outputPath = reducerOutput.getOutputPath();
						
			msg.addTaskOutput(new TaskOutput(nodeIp, outputPath));
		}
		
		return msg;
	}

	/**
	 * Helper method to validate job parameters.
	 * @param conf
	 */
	private void validateJobConf(JobConf conf) {
		
		int totalNumOfRegisteredNodes = slaveNodeManager.getTotalNumOfNodes();
		if(conf.getNumberOfReducers() > totalNumOfRegisteredNodes)
			conf.setNumberOfReducers(totalNumOfRegisteredNodes);
		
	}
	

	public void printNodeStates() {
		slaveNodeManager.printNodeStates();
	}

	/**
	 * Copies user code from the client to the master node.
	 * @param clientNodeIp
	 * @param outputPath
	 */
	private void saveClientJarToLocalDisk(String clientNodeIp, String outputDirectoryPath , String jarName , String jarPath) throws IOException
	{
		Socket downloadFileSocket = new Socket(clientNodeIp,
				Sfs.getReplicaTransferPort());

		ObjectOutputStream outputStream = new ObjectOutputStream(
				downloadFileSocket.getOutputStream());
		//outputStream.writeObject(path);
		SfsCompactReplicaRequest request = new SfsCompactReplicaRequest();
		request.addPath(jarPath);
		outputStream.writeObject(request);
		
		outputStream.flush();

		Files.createDirectories(Paths.get(outputDirectoryPath));

		String filePath = outputDirectoryPath + jarName + ".jar";

		BufferedInputStream reader = new BufferedInputStream(
				(downloadFileSocket.getInputStream()));
		BufferedOutputStream fileOutputStream = new BufferedOutputStream(
				new FileOutputStream(filePath));

		byte[] buffer = new byte[downloadFileSocket.getReceiveBufferSize()];
		long total_bytes_read = 0;

		int read = -1;
		while ((read = reader.read(buffer)) > 0) {
			total_bytes_read += read;
			fileOutputStream.write(buffer, 0, read);
		}

		System.out.println(total_bytes_read + " bytes read.");
		
		fileOutputStream.close();
		reader.close();
		outputStream.close();

	}
	
	private void doMapTask(int jobId) throws Exception
	{
		List<InputSplit> mapperSplits = jobManager.getInputSplitsForMappers(jobId);
		
		// each split has a mapper Id.
		List<Integer> mapperIds = slaveNodeManager.assignMappersToTasks(mapperSplits);
				
		assert(mapperIds.size() == mapperSplits.size());
		
		// send each mapper a message to start its map task.
		for(int i = 0 ; i < mapperIds.size() ; i++ )
		{
			int mapperId = mapperIds.get(i);
			InputSplit inputSplit = mapperSplits.get(i);
			
			// get a taskId for this job.
			int taskId = jobManager.getMapTaskId(jobId);
			
			System.out.println("Starting job on mapper with node Id " + mapperId + " for task " + taskId);
			
			// add the map task to this job's state to track.
			MapTask thisTask = new MapTask();
			thisTask.setInputSplit(inputSplit);
			thisTask.setNodeId(mapperId);
			thisTask.setTaskId(taskId);
			thisTask.setLastUpdateReceivedTime(System.currentTimeMillis());
			
			// one more task thats running on this node.
			//	slaveNodeManager.incrementNumOfMapTasksOnNode(mapperId);
			//keep track of this new task.
			jobManager.addMapTask(jobId , thisTask);
			
			// send a message to the selected slave node to do the map task.
			StartMapTaskMsg startMapTask = new StartMapTaskMsg( jobId , taskId, jobManager.getJobConf(jobId) , inputSplit , 
					jobManager.getWorkingDirectoryForJob(jobId));
			startMapTask.setMessageType(MESSAGE_TYPE.START_MAP_TASK);
			
			CommManager.sendMessageUsingCachedSocket(slaveNodeManager.getSlaveHostConnectionInfo(mapperId), startMapTask);
			
			
		}
		
		// all the map tasks have been started successfully on the mappers.
		
		jobManager.updateJobStatus(jobId, JobState.MAP_TASKS_RUNNING);
		System.out.println("All the map tasks have neen started successfully");
				
	}
	
	/**
	 * Waits for the map tasks that have been started to finish. In case it does not see an update from a slave node
	 * on a map task, takes corrective action by restarting the job on a different node.
	 * @param jobId
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void waitForMapTasksToFinish(final int jobId) throws InterruptedException, ExecutionException 
	{
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		
		while (! executor.isTerminated() ) 
		{	
			ScheduledFuture<Boolean> scheduledFuture = executor.schedule(new Callable<Boolean>() 
			{
				
						@Override
						public Boolean call() throws Exception 
						{
							// these are the map tasks for this job.
							Collection<MapTask> mapTasks = jobManager.getMapTasks(jobId);
							
							boolean shouldTerminatePoller = true;
							for(MapTask mapTask : mapTasks)
							{
								// if even one of the tasks is not finished, we have to continue;
								System.out.println(String.format("Map task %d running on host %s has completed %.2f of total progress" ,
										mapTask.getTaskId() , slaveNodeManager.getSlaveHostConnectionInfo(mapTask.getNodeId()).getIpAddress(),
										mapTask.getProgress()));
								
								// if any task has not completed, continue waiting.
								if(mapTask.getProgress() != 1.0)
								{
									shouldTerminatePoller = false;
								}
								
								// take corrective action in case any map task has stalled.
								// restart on a different node
								long diff = System.currentTimeMillis() - mapTask.getLastUpdateReceivedTime();
								//System.out.println(diff);
								if( !shouldTerminatePoller &&  ( diff ) > MAP_TASK_TIMEOUT )
								{
									System.out.println("-------------------------------");

									// node failure case. No update received for timeout period.
									System.out.println("Trying to fix map task failure on " + mapTask.getNodeId() + " doing task with taskId " + mapTask.getTaskId());
									
									System.out.println("-------------------------------");
																		
									
									// get a different mapper on which to start this task.
									int newMapperNodeId = slaveNodeManager.getAlternateMapperForTask(mapTask);
									
									System.out.println("Fixing map task with task id " + mapTask.getTaskId() + " by restarting it on node " + newMapperNodeId );
									
									// send a message to the selected slave node to do the map task.
									StartMapTaskMsg startMapTask = new StartMapTaskMsg( jobId , mapTask.getTaskId(), jobManager.getJobConf(jobId) , mapTask.getInputSplit() , 
											jobManager.getWorkingDirectoryForJob(jobId));
									startMapTask.setMessageType(MESSAGE_TYPE.START_MAP_TASK);
									
									CommManager.sendMessageUsingCachedSocket(slaveNodeManager.getSlaveHostConnectionInfo(newMapperNodeId), startMapTask);
									
									// update the node that was doing this map task.
									mapTask.setNodeId(newMapperNodeId);
									mapTask.setLastUpdateReceivedTime(System.currentTimeMillis());
									// update this task in the local store.
									jobManager.addMapTask(jobId , mapTask);
									 // make sure we do not terminate yet, as we need to complete this task.
									shouldTerminatePoller = false;
											
								}
								
							}
							
							return shouldTerminatePoller;
						}
			}, 1, TimeUnit.SECONDS);
			
			if(scheduledFuture.get().equals(Boolean.TRUE))
			{
				// we are done. shutdown now.`
				System.out.println("Stop polling now");
				scheduledFuture.cancel(false);
				executor.shutdown();
				executor.awaitTermination(5, TimeUnit.SECONDS);
				System.out.println("All map tasks have been completed successfully");
				jobManager.updateJobStatus(jobId,JobState.MAP_TASKS_FINISHED);
			}
		}
		
		
		
	}
	
	/**
	 * Starts off the required number of reducers needed for this map reduce job.
	 * @param jobId
	 * @throws Exception
	 */
	private void doReduceTask(int jobId) throws Exception
	{
		Map<Integer, List<TaskOutput>> reducerInputs = jobManager.getReducerInputPaths(jobId);
		
		List<Integer> reducerIds = slaveNodeManager.getAvailableReducers( reducerInputs , reducerInputs.size());
		
		System.out.println("Chosen reducers for this task are");
		for(int i : reducerIds)
			System.out.print(i + " ");
		System.out.println();
		
		// for each partition, start off a new reducer.
		int cnt = 0;
		for(Integer partitionNumber : reducerInputs.keySet() )
		{
			List<TaskOutput> filesInThisPartition = reducerInputs.get(partitionNumber);
			
			int taskId = jobManager.getReduceTaskId(jobId);
			
			StartReduceTaskMsg startReduceTaskMsg = new StartReduceTaskMsg();
			startReduceTaskMsg.setJobId(jobId);
			startReduceTaskMsg.setTaskId(taskId);
			startReduceTaskMsg.setJobConf(jobManager.getJobConf(jobId));
			startReduceTaskMsg.setMessageType(MESSAGE_TYPE.START_REDUCE_TASK);
			startReduceTaskMsg.setReducerInputPaths(filesInThisPartition);
			startReduceTaskMsg.setMasterLocalWorkingDir(jobManager.getWorkingDirectoryForJob(jobId));
			
			int reducerId = reducerIds.get(cnt++);
			
			ReduceTask thisTask = new ReduceTask();
			thisTask.setNodeId(reducerId);
			thisTask.setTaskId(taskId);
			thisTask.setInputPaths(filesInThisPartition);
			thisTask.setLastUpdateReceivedTime(System.currentTimeMillis());
			
			jobManager.addReduceTask(jobId , thisTask);
			System.out.println("Reduce task " + taskId + " starting on host " + slaveNodeManager.getSlaveHostConnectionInfo(reducerId).getIpAddress());
			
			CommManager.sendMessageUsingCachedSocket(slaveNodeManager.getSlaveHostConnectionInfo(reducerId), startReduceTaskMsg);
			
		}
		
		jobManager.updateJobStatus(jobId, JobState.REDUCE_TASKS_RUNNING);
		System.out.println("All the reduce tasks have been started successfully");
		
	}
	
	/**
	 * This method blocks until all the reduce tasks have finished. It also handles reducer node
	 * failure , by restarting the task on a different node.
	 * @param jobId
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void waitForReduceTasksToFinish(final int jobId) throws InterruptedException, ExecutionException {
		
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		
		while (! executor.isTerminated() ) 
		{	
			ScheduledFuture<Boolean> scheduledFuture = executor.schedule(new Callable<Boolean>() 
			{

						@Override
						public Boolean call() throws Exception 
						{
							// these are the reduce tasks for this job.
							Collection<ReduceTask> reduceTasks = jobManager.getReduceTasks(jobId);
							
							boolean shouldTerminatePoller = true;
							
							for(ReduceTask reduceTask : reduceTasks)
							{	
								System.out.println(String.format("Reduce task %d running on host %s has completed %.2f of total progress" ,
										reduceTask.getTaskId() , slaveNodeManager.getSlaveHostConnectionInfo(reduceTask.getNodeId()).getIpAddress(),
										reduceTask.getProgress()));
								// if even one of the tasks is not finished, we have to continue;
								if(reduceTask.getProgress() != 1.0)
								{
									shouldTerminatePoller = false;
								}
								
								if( !shouldTerminatePoller &&  ((System.currentTimeMillis() - reduceTask.getLastUpdateReceivedTime() ) > REDUCE_TASK_TIMEOUT ) )
								{
									
									// node failure case. No update received for timeout period.
									System.out.println("-------------------------------");
									
									System.out.println("Trying to fix reducer failure on " + reduceTask.getNodeId() + " doing task with taskId " + reduceTask.getTaskId());
									
									int newReducerId = slaveNodeManager.getNewReducerForTask(reduceTask);
									
									System.out.println("Fixing reducer task with task id " + reduceTask.getTaskId() + " by restarting it on node " + newReducerId );

									
									StartReduceTaskMsg startReduceTaskMsg = new StartReduceTaskMsg();
									startReduceTaskMsg.setJobId(jobId);
									startReduceTaskMsg.setTaskId(reduceTask.getTaskId());
									startReduceTaskMsg.setJobConf(jobManager.getJobConf(jobId));
									startReduceTaskMsg.setMessageType(MESSAGE_TYPE.START_REDUCE_TASK);
									startReduceTaskMsg.setReducerInputPaths(reduceTask.getInputPaths());
									startReduceTaskMsg.setMasterLocalWorkingDir(jobManager.getWorkingDirectoryForJob(jobId));
									
									CommManager.sendMessageUsingCachedSocket(slaveNodeManager.getSlaveHostConnectionInfo(newReducerId), startReduceTaskMsg);

									reduceTask.setNodeId(newReducerId);
									reduceTask.setProgress(0.0);
									reduceTask.setLastUpdateReceivedTime(System.currentTimeMillis());
									// update this task in the local store.
									jobManager.addReduceTask(jobId , reduceTask);
									
									shouldTerminatePoller = false;
									
								}
							}
							
							return shouldTerminatePoller;
						}
			}, 1 , TimeUnit.SECONDS);
			
			if(scheduledFuture.get().equals(Boolean.TRUE))
			{
				// we are done. shutdown now.
				scheduledFuture.cancel(false);
				executor.shutdown();
				executor.awaitTermination(5 , TimeUnit.SECONDS);
				jobManager.updateJobStatus(jobId, JobState.REDUCE_TASKS_FINISHED);
				System.out.println("All reduce tasks have been completed successfully");
			}
		}
		
	}
	
	
	class MasterServingThread implements Runnable
	{
		ObjectInputStream inputStream;
		ObjectOutputStream outputStream;
		ServerSocket serverSocket;
		
		public MasterServingThread(int servingPort) throws Exception
		{
			serverSocket = new ServerSocket(servingPort);
		}

		@Override
		public void run() 
		{
			
			while (true) 
			{
				try 
				{
					Socket socket = serverSocket.accept();
					
					outputStream = new ObjectOutputStream(
							socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());

					//read incoming message.
					Message message = (Message) inputStream.readObject();
					MESSAGE_TYPE messageType = message.getMessageType();
					
					if( messageType == null)
					{
						throw new IllegalArgumentException("No message Type is set.");
					}
					
					// check if this is a valid message type.					
					if(messageType.equals(MESSAGE_TYPE.REGISTER_WITH_SERVER))
					{
						HostInfo hostInfo = new HostInfo();
						hostInfo.setIpAddress(socket.getInetAddress().getHostAddress());
						hostInfo.setObjectInputStream(inputStream);
						hostInfo.setObjectOutputStream(outputStream);
						hostInfo.setPort(socket.getPort());
						hostInfo.setSfsPort((int) message.getMessage());
						
						 
						// add this node to MR master store.
						int nodeId = slaveNodeManager.addAndCacheNodeConnection(hostInfo);
						//TODO add a type for this message.
						Message reply = new Message();
						// send the slave a node Id.
						reply.setMessage(nodeId);
						outputStream.writeObject(reply);
					}
					else if(messageType.equals(MESSAGE_TYPE.START_NEW_MR_JOB))
					{
						final NewMRJobMsg newJobMessage = (NewMRJobMsg)message;
						outputStream.writeObject(Util.getAckMessage());
						
						masterExecutorService.submit(new Runnable() {
							
							@Override
							public void run() {
									startNewMapReduceJob(newJobMessage);
							}
						});
						
						
					}
					else if(messageType.equals(MESSAGE_TYPE.MAP_TASK_STATUS_MSG))
					{
						outputStream.writeObject(Util.getAckMessage());
						MapTaskStatusMsg msg = (MapTaskStatusMsg) message;
						
						
						int nodeId = msg.getNodeId();
						HostInfo mapperHostInfo = slaveNodeManager.getSlaveHostConnectionInfo(nodeId);
						jobManager.updateMapTaskStatus(msg , mapperHostInfo.getIpAddress());
						
						if(msg.getException() == null && msg.getProgress() == 1.0)
						{
							// task complete.
							slaveNodeManager.decrementNumOfMapTasksOnNode(msg.getNodeId());
						}
						
					}
					else if(messageType.equals(MESSAGE_TYPE.REDUCE_TASK_STATUS_MSG))
					{
						outputStream.writeObject(Util.getAckMessage());
						ReduceTaskStatusMsg msg = (ReduceTaskStatusMsg) message;
						
						jobManager.updateReduceTaskStatus(msg);
						
						if(msg.getException() == null && msg.getProgress() == 1.0)
						{
							// task complete.
							//System.out.println("The reducer output path is" + msg.getOutputPath());
							slaveNodeManager.decrementNumOfReduceTasksOnNode(msg.getNodeId());
						}
						
					}
						
				}
				catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
		
		}
		
	}



}
