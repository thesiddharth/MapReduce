package mapreduce.slave;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mapreduce.data.CombineTaskResult;
import mapreduce.data.TaskOutput;
import mapreduce.data.Entry;
import mapreduce.data.HostInfo;
import mapreduce.data.JobConf;
import mapreduce.data.MapReduceException;
import mapreduce.data.PQueueEntry;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.data.SfsCompactReplicaRequest;
import mapreduce.impl.MapOutputCollector;
import mapreduce.impl.ReduceOutputCollector;
import mapreduce.input.CombinedFileEntry;
import mapreduce.input.CombinedMapOutputReader;
import mapreduce.input.FileInputSplit;
import mapreduce.input.KeyValueLineRecordReader;
import mapreduce.input.LineRecordReader;
import mapreduce.interfaces.Mapper;
import mapreduce.interfaces.Reducer;
import mapreduce.interfaces.WritableComparable;
import mapreduce.util.ConfigManager;
import mapreduce.util.Util;

import comm.Message;
import comm.Message.MESSAGE_TYPE;
import comm.StartMapTaskMsg;
import comm.StartReduceTaskMsg;

/**
 * Represents the functionality of a slave node on the network.
 * @author surajd
 *
 */
public class SlaveNode {

	private static final String TASK_WORKING_DIRECTORY = "/tmp/mapReduce/";
    private static final String MAPPER_OUTPUT_PATH = "mapper_output/";
    private static final String REDUCER_INPUT = "tempReducerFiles/";
    private static final String COMBINED_OUTPUT_DIRECTORY = "tempCombinedOutput/";
	private static final int MAX_RETRY_COUNTS = 3;
    
	// node Id assinged to this worker by the master.
	private int nodeId;
	//address of the master node.
	private HostInfo masterNodeHostInfo;
	private ExecutorService mapTaskExecutor;
	private ExecutorService reduceTaskExecutor;
	
	
	public SlaveNode(String configFilePath)
	{ 
		// read config parameters.
		ConfigManager.init(configFilePath);

        String masterIP = Util.getStringConfigValue(Util.MASTER_IP_ADDRESS_KEY);
        int masterPort =  Util.getIntConfigValue(Util.MR_MASTER_LISTENING_PORT_KEY);
        int masterHeartBeatPort = Util.getIntConfigValue(Util.MASTER_HEART_BEAT_PORT_KEY);
        int sfsListeningPort = Util.getIntConfigValue(Util.SFS_WORKER_NODE_PORTs);
        System.out.println("Master ip " + masterIP + " and port " + masterPort);
		masterNodeHostInfo = new HostInfo ( masterIP , masterPort );
		
		new WorkerHeartbeat(masterIP, masterHeartBeatPort).startPolling();;
		// start a new thread to server commands from the master.
		new Thread( new SlaveNodeRequestHandler(masterNodeHostInfo , sfsListeningPort)).start();
		
		
		// max number of map tasks that can concurrently occur.
        int numberOfMappers =  Util.getIntConfigValue(Util.MAX_NUMBER_OF_MAP_TASKS);
        // max number of reduce tasks that can concurrently occur.
		int numberOfReducers =  Util.getIntConfigValue(Util.MAX_NUMBER_OF_REDUCE_TASKS);
		
		// service to handle map tasks.
		mapTaskExecutor = Executors.newFixedThreadPool(numberOfMappers);
		// service to handle reduce tasks.
		reduceTaskExecutor = Executors.newFixedThreadPool(numberOfReducers);
		
	}
	
	/**
	 * Starts a new map task on this node.
	 * @param message
	 * @throws MapReduceException
	 */
	private void startNewMapTask(StartMapTaskMsg message)
	{
			System.out.println("Starting map task with id " + message.getTaskId() + " for job " + message.getJobId());
			// job for which we are doing this map task.
			// this is unique at a system level.
			int jobId = message.getJobId();
			JobConf jobConf = message.getJobConf();
			int thisMapTaskId = message.getTaskId();
			
			Reporter reporter = new Reporter( masterNodeHostInfo , nodeId , jobId , thisMapTaskId);
			int retryCount = 0;
			boolean notDone = true;
			
			while (notDone && retryCount < MAX_RETRY_COUNTS) 
			{
				try 
				{
					String taskWorkingDirectory = TASK_WORKING_DIRECTORY
							+ jobId + "/" + thisMapTaskId + "/";

					String remoteJarPath = message.getLocalMasterJobDir()
							+ jobConf.getJarName() + ".jar";
					String localJarDirectory = taskWorkingDirectory;
					String jarFileName = jobConf.getJarName() + ".jar";

					//copy over user code from the master.
					getRemoteFile(masterNodeHostInfo.getIpAddress(),remoteJarPath, localJarDirectory, jarFileName);
					
					System.out.println("Got jar from mapper at "
							+ localJarDirectory + jarFileName);
					// run the map task now.
					FileInputSplit inputSplit = (FileInputSplit) message.getInputSplit();
					
					System.out.println("Reading split at input split "
							+ inputSplit.getFilePath());

					LineRecordReader reader = new LineRecordReader(inputSplit);
					double taskProgress = 0;
					// added a one here to not make it 100.
					double delta = 1.0 / (inputSplit.getLengthOfSplit() + 1); // jump of each record.
					double deltaThreshold = 0.10; // threshold beyond which we will make an update.
					double currentThreshold = 0.0; // delta threshold.

					MapOutputCollector<? extends WritableComparable<?>, ? extends WritableComparable<?>> outputCollector = new MapOutputCollector(jobConf.getNumberOfReducers());

					// report that the task has started.
					reporter.reportMapperProgress(0);

					Class<?> mapClass = getMapperFromUserJar(localJarDirectory + jarFileName);
					
					Class<?>[] parameterTypes = new Class[] {jobConf.getMapInputKey(), jobConf.getMapInputValue(), MapOutputCollector.class };
					
					Object mapClassInstance = mapClass.newInstance();
					Method method = mapClass.getMethod("map", parameterTypes);

					// run the map method on each record.
					while (reader.hasNextEntry()) 
					{
						Entry<? extends WritableComparable<?>, ? extends WritableComparable<?>> entry = reader.getNextEntry();
						
						Object[] args = new Object[] { entry.getKey(), entry.getValue(), outputCollector };
						
						method.invoke(mapClassInstance, args);

						// update progress.
						taskProgress = taskProgress + delta;

						// update master with the progress.
						if (taskProgress >= currentThreshold) {
							//System.out.println("Reporting map task progress to master " + String.format("%.2f", taskProgress * 100.0));
							reporter.reportMapperProgress(taskProgress);
							currentThreshold += deltaThreshold;
						}
					}

					// map task here has finished.
					// parse output collector's output.
					// create partitions - one for each reducer
					// write the records to their respective partitions.
					// update the master with the locations of the partitions.

					// map between partition number and entries that are mapped to that partition.
					Map<Integer, ?> mapOutputValues = outputCollector.retrieveCollectedValues(reporter, taskProgress);
					
					System.out.println("Done processing records,  will write output to disk now");
					Map<Integer, String> mapperOutputPaths = new HashMap<>();

					for (Integer partition : mapOutputValues.keySet()) 
					{
						System.out.println("Writing partition " + partition + " to disk");
						
						//report some dummy progress , so the master knows I am alive.
						reporter.reportMapperProgress(taskProgress);
						List<Entry> keyValues = (List<Entry>) mapOutputValues.get(partition);
						
						String partitionFileName = "part-" + Util.getUniqueFileName();
						
						writeMapperOutputToLocalDisk(taskWorkingDirectory
								+ MAPPER_OUTPUT_PATH, partitionFileName,
								partition, keyValues, reporter, taskProgress);

						mapperOutputPaths.put(partition, taskWorkingDirectory
								+ MAPPER_OUTPUT_PATH + partitionFileName);
						// report some progress to the master.
						reporter.reportMapperProgress(taskProgress);
					}

					reporter.reportMapperProgress(1.0, mapperOutputPaths);

					reader.close();

					// this retry attempt has gone through.
					notDone = false;
				} 
				catch (Exception e) 
				{
					retryCount++;
					e.printStackTrace();
					// if we have exhausted all retries, exit.
					if(retryCount == MAX_RETRY_COUNTS)
						reporter.reportMapperProgress(new MapReduceException(e));
					// try after some more time.
					try {
						Thread.sleep(retryCount * 1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} 
				}
			}
	}
	
	/**
	 * Parses the jar and returns the mapper class.
	 * @param localJarPath
	 * @return
	 * @throws ClassNotFoundException 
	 */
	private Class<?> getMapperFromUserJar(String localJarPath) throws MapReduceException
	{
		return Util.readClassFromJar(localJarPath, Mapper.class, "Map");
	}
	
	/**
	 * Parses the jar and reads the reducer class from it.
	 * @param localJarPath
	 * @return
	 * @throws MapReduceException
	 */
	private Class<?> getReducerFromUserJar(String localJarPath ) throws MapReduceException
	{
		return Util.readClassFromJar(localJarPath, Reducer.class, "Reduce");
	}


	/**
	 * Starts a reduce task on this worker node.
	 * @param message
	 * @param thisReduceTaskId
	 * @throws Exception
	 */
	public void startNewReduceTask(StartReduceTaskMsg message) 
	{
			JobConf jobConf = message.getJobConf();
			int jobId = message.getJobId();
			int thisReduceTaskId = message.getTaskId();
			
			// reports status of the reduce task to the master
			Reporter reporter = new Reporter(masterNodeHostInfo, nodeId, jobId, thisReduceTaskId);
			
			String taskWorkingDirectory = TASK_WORKING_DIRECTORY + jobId + "/" + thisReduceTaskId + "/";
			
			int retryCount = 0;
			boolean notDone = true;
			while (notDone && retryCount < MAX_RETRY_COUNTS) 
			{
				try {

					String remoteJarPath = message.getMasterLocalWorkingDir()
							+ jobConf.getJarName() + ".jar";
					String localJarDirPath = taskWorkingDirectory;
					String localJarFileName = jobConf.getJarName() + ".jar";

					//copy over user code from the master.
					getRemoteFile(masterNodeHostInfo.getIpAddress(),
							remoteJarPath, localJarDirPath, localJarFileName);
					
					System.out.println("Got jar from master at location "
							+ localJarDirPath + localJarFileName);
					//combine these files into one file on the local disk.

					double progress = 0;
					CombineTaskResult result = combineMapperOutputFiles(jobId,
							thisReduceTaskId, message.getReducerInputPaths(),
							taskWorkingDirectory, reporter, progress);
					double delta = (1.0 / (result.getNumberOfRecords() + 1.0));
					double deltaThreshold = 0.10; // threshold beyond which we will make an update.
					double currentThreshold = 0.0; // delta threshold.

					Class<?> reduceClass = getReducerFromUserJar(localJarDirPath + localJarFileName);
					
					Class<?>[] parameterTypes = new Class[] {
							jobConf.getReduceInputKey(), Iterator.class,
							ReduceOutputCollector.class };
					
					Object reduceClassInstance = reduceClass.newInstance();
					Method method = reduceClass.getMethod("reduce",
							parameterTypes);

					ReduceOutputCollector<? extends WritableComparable<?>, ? extends WritableComparable<?>> outputCollector = new ReduceOutputCollector<>();

					CombinedMapOutputReader reader = new CombinedMapOutputReader(result.getOutputFilePath() , jobConf.getReduceInputValue());

					// run the reducer on each intermediate map output.
					while (reader.hasNextEntry()) {
						CombinedFileEntry entry = reader.getNextEntry();
						Object[] args = new Object[] { entry.getKey(),
								entry.getIterator(), outputCollector };
						method.invoke(reduceClassInstance, args);
						progress += delta;

						// update master with the progress.
						if (progress >= currentThreshold) {
							reporter.reportReducerProgress(progress);
							currentThreshold += deltaThreshold;
						}

					}

					Collection<Entry<? extends WritableComparable<?>, ? extends WritableComparable<?>>> reducerOutput = outputCollector.retrieveCollectedValues();

					// write reducer output to file.
					String outputPath = jobConf.getOutputPath();
					// create the output directory.
					File file = new File(outputPath);
					file.mkdirs();

					String reducerOutputPath = outputPath + jobId + "-" + nodeId + "-" + Util.getUniqueFileName();

					PrintWriter writer = new PrintWriter(new BufferedWriter(
							new FileWriter(reducerOutputPath)));

					int cnt = 0;

					for (Entry<? extends WritableComparable<?>, ? extends WritableComparable<?>> e : reducerOutput) {
						e.getKey().writeTo(writer);
						writer.print(" ");
						e.getValue().writeTo(writer);
						writer.println();

						if (cnt % 100000 == 0) {
							reporter.reportReducerProgress(progress);
							cnt++;
						}
					}

					writer.close();
					System.out.println("Writing reducer output to "
							+ reducerOutputPath);
					reporter.reportReducerProgress(1.0, reducerOutputPath);
					notDone = false;
				} 
				catch (Exception e) 
				{
					retryCount++;
					e.printStackTrace();
					// if we have exhausted all retries, exit.
					if(retryCount == MAX_RETRY_COUNTS)
						reporter.reportReducerProgress(new MapReduceException(e));
					
					// try after some more time.
					try {
						Thread.sleep(retryCount * 1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} 
				}
			}
		
		
	}
		
	
	/**
	 * Combines the remote , sorted mapper outputs to a single file on local dist,
	 * Returns the combined file to be sent for reduce step.
	 * @param jobId
	 * @param taskWorkingDirectory 
	 * @param outputPaths
	 * @return
	 * @throws Exception
	 */
	public CombineTaskResult combineMapperOutputFiles(int jobId , int taskId,  List<TaskOutput> completedRemotePaths, String taskWorkingDirectory,
			Reporter reporter, double progress) throws Exception
	{
		CombineTaskResult ret = new CombineTaskResult();
		
		int numberOfRecords = 0;
		
		List<String> outputPaths = getRemoteMapperOutputFiles(completedRemotePaths , taskWorkingDirectory);
		
		// readers to read each file.
		KeyValueLineRecordReader[] recordReaders = new KeyValueLineRecordReader[outputPaths.size()];
		
		// init readers.
		for(int i = 0 ; i < recordReaders.length ; i++)
		{
			recordReaders[i] = new KeyValueLineRecordReader(outputPaths.get(i));
		}
		
		// priority queue to combine mapper outputs.
		PriorityQueue<PQueueEntry<? extends WritableComparable<?> , ? extends WritableComparable<?> >> pQueue = new PriorityQueue<>(recordReaders.length
				, new Comparator<PQueueEntry>() {

			@Override
			public int compare(PQueueEntry o1, PQueueEntry o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		
		// init pQueue with first record from each file.
		for(int i = 0 ; i < recordReaders.length ; i++)
		{
				if(recordReaders[i].hasNextEntry())
				{
					pQueue.add(new PQueueEntry<>(i, recordReaders[i].getNextEntry()));
				}
		}

        String path = TASK_WORKING_DIRECTORY + jobId + "/" + taskId + "/" + COMBINED_OUTPUT_DIRECTORY ;
        Files.createDirectories(Paths.get(path));
        String filePath = path + Util.getUniqueFileName();
       
        // write the combined output to local disk.
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath) ));
		
		while(! pQueue.isEmpty() )
		{
			numberOfRecords++;
			
			if(numberOfRecords % 1000000 == 0)
			{
				reporter.reportReducerProgress(progress);
			}
			// smallest entry in the current pQueue.
			PQueueEntry pQueueEntry = pQueue.poll();

			int idx = pQueueEntry.getIdx();
			
			// write this value.
			WritableComparable key = pQueueEntry.getKey();
			WritableComparable value = pQueueEntry.getValue();

			StringBuilder builder = new StringBuilder();
			
			builder.append(key.toString());
			builder.append(" ");
			builder.append(value.toString());
	
			// add the next entry from this file to the pQueue.
			if (recordReaders[idx].hasNextEntry()) 
			{
				pQueue.add(new PQueueEntry<>(idx, recordReaders[idx]
						.getNextEntry()));
			}
			
			// now keep reading the same key from the pQueue.
			while(!pQueue.isEmpty() && pQueue.peek().getKey().equals(key))
			{
				PQueueEntry nextEntry = pQueue.poll();
				
				WritableComparable nextKey = nextEntry.getKey();
				WritableComparable nextValue = nextEntry.getValue();
				int nextIdx = nextEntry.getIdx();
				
				assert( nextKey.equals(key) );
				
				builder.append(",");
				builder.append(nextValue.toString());
				

				if (recordReaders[nextIdx].hasNextEntry()) 
				{
					pQueue.add(new PQueueEntry<>(nextIdx, recordReaders[nextIdx]
							.getNextEntry()));
				}
			}
			writer.println(builder.toString());
		}
		
		writer.close();
		
		// return the result.
		ret.setNumberOfRecords(numberOfRecords);
		ret.setOutputFilePath(filePath);
		
		return ret;
		
	}
	
	/**
	 * Downloads the remote mapper output files to local disk.
	 * @param remoteOutputPaths
	 * @param taskWorkingDirectory
	 * @return
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	private List<String> getRemoteMapperOutputFiles(List<TaskOutput> remoteOutputPaths, String taskWorkingDirectory) throws UnknownHostException, IOException
	{
		List<String> localPaths = new ArrayList<>();
		int cnt = 0;

		for (TaskOutput remotePath : remoteOutputPaths) {
			String ip = remotePath.getIpAddress();
			String path = remotePath.getFilePath();
			
			String localPath = taskWorkingDirectory + REDUCER_INPUT;
            String filePath = String.valueOf(cnt++);
					
			getRemoteFile(ip, path,  localPath , filePath);

			localPaths.add(localPath+filePath);
		}

		return localPaths;

	}
	
	/**
	 * Writes output of the map task to the local disk of a mapper node.
	 * @param directoryPath
	 * @param partitionOutputPath
	 * @param partition
	 * @param values
	 * @throws Exception 
	 */
	private void writeMapperOutputToLocalDisk(String directoryPath, String partitionOutputPath,  int partition , List<Entry> values ,
			Reporter reporter , double progress) throws Exception 
	{
		System.out.println("Writing mapper output to directory " + directoryPath + " with fileName " + partitionOutputPath);
		
		File file = new File(directoryPath);
		file.mkdirs();
		
		PrintWriter writer = new PrintWriter(new BufferedWriter( new FileWriter(directoryPath+partitionOutputPath) ) );
		int cnt = 0;
		for( Entry entry : values)
		{
			entry.getKey().writeTo(writer);
			writer.print(" ");
			entry.getValue().writeTo(writer);
			writer.println();
			
			cnt++;
			if(cnt == 1000000)
			{
				reporter.reportMapperProgress(progress);
				//System.out.println("Reporting mapper progress while writing");
				cnt = 0;
			}
		}
		
		writer.close();
	}
	
	/**
	 * Downloads a file from a different machine.
	 * @param remoteIp
	 * @param remotePath
	 * @param localDirPath
	 * @param localFileName
	 * @throws IOException
	 */
	private void getRemoteFile(String remoteIp , String remotePath, String localDirPath , String localFileName) throws IOException
	{
		Socket downloadFileSocket = new Socket(remoteIp,Sfs.getReplicaTransferPort());

		ObjectOutputStream outputStream = new ObjectOutputStream(downloadFileSocket.getOutputStream());
		SfsCompactReplicaRequest request = new SfsCompactReplicaRequest();
		request.addPath(remotePath);
		
		outputStream.writeObject(request);

		outputStream.flush();

		// create the directory if it exists.
		new File(localDirPath).mkdirs();

		BufferedInputStream reader = new BufferedInputStream(
				(downloadFileSocket.getInputStream()));
		BufferedOutputStream fileOutputStream = new BufferedOutputStream(
				new FileOutputStream(localDirPath + localFileName));

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
	
	
	/**
	 * Opens a channel of communication with the master node. Accepts incoming commands,
	 * and takes appropriate action.
	 * @author surajd
	 *
	 */
	class SlaveNodeRequestHandler implements Runnable
	{
		private Socket socket;
		private ObjectInputStream objectInputStream;
		private ObjectOutputStream objectOutputStream;
		
		public SlaveNodeRequestHandler(HostInfo masterNodeHostInfo, int sfsListeningPort)
		{
			try 
			{
				socket = new Socket(masterNodeHostInfo.getIpAddress(), masterNodeHostInfo.getPort());
				objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(Util.getRegisterWithServerMessage(sfsListeningPort));
				objectInputStream = new ObjectInputStream(socket.getInputStream());
				Message reply = (Message)objectInputStream.readObject();
				nodeId = (int) reply.getMessage();
				
				masterNodeHostInfo.setObjectInputStream(objectInputStream);
				masterNodeHostInfo.setObjectOutputStream(objectOutputStream);
				
			} 
			catch (Exception e)
			{
				System.err.println("Error while registering with the server");
				e.printStackTrace();
			} 
		}
		
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					
					final Message message = (Message) objectInputStream.readObject();
										
					if (message.getMessageType().equals(MESSAGE_TYPE.START_MAP_TASK)) 
					{
						objectOutputStream.writeObject(Util.getAckMessage());	
						mapTaskExecutor.submit(new MapTaskHandler((StartMapTaskMsg)message));
					}
					else if (message.getMessageType().equals(MESSAGE_TYPE.START_REDUCE_TASK)) 
					{
						objectOutputStream.writeObject(Util.getAckMessage());
						reduceTaskExecutor.submit(new ReduceTaskHandler((StartReduceTaskMsg)message));
					}
					
				} 
				catch (IOException | ClassNotFoundException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}
				
			}	
			
		}
		
	}
	
	class MapTaskHandler implements Runnable
	{
		private StartMapTaskMsg message;
		public MapTaskHandler(StartMapTaskMsg message) 
		{
			this.message = message;
		}

		@Override
		public void run() {
			startNewMapTask(message);
			
		}
		
	}
	
	class ReduceTaskHandler implements Runnable
	{
		private StartReduceTaskMsg message;
		public ReduceTaskHandler(StartReduceTaskMsg message) 
		{
			this.message = message;
		}

		@Override
		public void run() {
			startNewReduceTask(message);
			
		}
		
	}
}
