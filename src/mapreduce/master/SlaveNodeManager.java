/**
 * 
 */
package mapreduce.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import mapreduce.data.TaskOutput;
import mapreduce.data.HostInfo;
import mapreduce.data.MapTask;
import mapreduce.data.ReduceTask;
import mapreduce.data.SlaveNodeInfo;
import mapreduce.data.SlaveNodeInfo.SlaveNodeState;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.SfsFileInfo;
import mapreduce.dfs.SfsSegmentInfo;
import mapreduce.dfs.CommunicationManager.CommunicationManager;
import mapreduce.dfs.data.SfsAddOrRemoveNodeMessage;
import mapreduce.dfs.data.SfsException;
import mapreduce.dfs.data.SfsFileDoesNotExistException;
import mapreduce.dfs.data.SfsMessage;
import mapreduce.input.InputSplit;
import mapreduce.util.Util;

import comm.HeartbeatMsg;

/**
 * Manager class storing relevant information about the nodes in the MR cluster.
 * In particular stores the number of map and reduces tasks running
 * on each node.  
 * @author surajd
 *
 */
public class SlaveNodeManager {
	
	private static final long SLAVE_TIMEOUT_PERIOD = 20 * 1000L;
	// map between node Id and MR info.
	private Map<Integer , SlaveNodeInfo> nodeIdToMRInfo;
	// map between nodeId and location on the network, cache connections.
	private Map<Integer, HostInfo> nodeIdToNodeInfo;
	//counter used to name nodes.
	private int nodeCounter;
	
	private HostInfo sfsMasterHostInfo;
	
	public SlaveNodeManager() throws IOException
	{
		nodeIdToMRInfo = new ConcurrentHashMap<>();
		nodeIdToNodeInfo = new ConcurrentHashMap<>();
		nodeCounter = 0;
		
		sfsMasterHostInfo = new HostInfo();
		sfsMasterHostInfo.setIpAddress(Util.getStringConfigValue(Util.MASTER_IP_ADDRESS_KEY));
		sfsMasterHostInfo.setPort(Util.getIntConfigValue(Util.SFS_MASTER_LISTENING_PORT));
		
		// start heartbeat listener from nodes.
		int heartBeatPort = Util.getIntConfigValue(Util.MASTER_HEART_BEAT_PORT_KEY);
		new Thread(new HeartBeatManager(heartBeatPort)).start();
		
		// periodically update status of nodes in the network.
		startSlaveNodeStatusUpdater();
	}
	
	/**
	 * Updates the state of each slave node based on received timestamps.
	 */
	private void startSlaveNodeStatusUpdater()
	{
		ScheduledExecutorService nodeStatusUpdater = Executors.newSingleThreadScheduledExecutor();
		
		nodeStatusUpdater.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				
				for(Integer nodeId : nodeIdToMRInfo.keySet())
				{
					if( (System.currentTimeMillis() - nodeIdToMRInfo.get(nodeId).getLastUpdateReceivedTimestamp()) >  SLAVE_TIMEOUT_PERIOD)
					{
						// this node has been unresponsive for a while.
						// mark its state as dead.
						try 
						{
							if (nodeIdToMRInfo.get(nodeId).getNodeState().equals(SlaveNodeState.ALIVE)) {
								nodeIdToMRInfo.get(nodeId).setNodeState(
										SlaveNodeState.DEAD);
								// update Sfs
								String ipAddress = nodeIdToNodeInfo.get(nodeId)
										.getIpAddress();
								int sfsPort = nodeIdToNodeInfo.get(nodeId)
										.getSfsPort();
								SfsAddOrRemoveNodeMessage removeMessage = new SfsAddOrRemoveNodeMessage(
										ipAddress, sfsPort);
								removeMessage
										.setType(SfsMessage.SfsMessageType.REMOVE_HOST);
								CommunicationManager.sendMessage(
										sfsMasterHostInfo, removeMessage);
							}
						} 
						catch (SfsException e) 
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
			}
		}, 5, 5, TimeUnit.SECONDS);
	}
	
	
	/**
	 * Returns a list of nodes on the network to perform the 
	 * required number of map tasks.
	 * @param numOfMapTasks
	 * @return
	 * @throws SfsFileDoesNotExistException 
	 */
	public List<Integer> assignMappersToTasks(List<InputSplit> inputSplits) throws SfsFileDoesNotExistException
	{
		assert(inputSplits.size() > 0);
		
		List<Integer> ret = new LinkedList<>();
		
		SfsFileInfo fileInfo = Sfs.getFileInfo(inputSplits.get(0).getActualFilePath());
		
		Map<Integer, LinkedHashSet<String>> map = fileInfo.getSegmentToHostListMap();
		
		assert( map.size() == inputSplits.size());		
				
		for(int i = 0 ; i < inputSplits.size() ; i++)
		{
			LinkedHashSet<String> hostIps = map.get(i);
			String selectedHost = getOptimalMapper(hostIps);
            System.out.println("Selected host " + selectedHost + " for input split " + inputSplits.get(i).getFilePath());
			ret.add(getNodeIdFromIp(selectedHost));
		}
		return ret;
	}
	
	/**
	 * Returns 
	 * @param ipList
	 * @return
	 */
	private String getOptimalMapper(LinkedHashSet<String> ipAddresses) {
		
		int leastNumOfMapTasks = 5000;
		String leastLoadedMapperIp = null;
		
		for(String ipAddress : ipAddresses)
		{
			int nodeId = getNodeIdFromIp(ipAddress);
			
			if(!isNodeAlive(nodeId))
				continue;
			
			int numOfRunningMapTasks = nodeIdToMRInfo.get(nodeId).getNumberOfRunningMapTasks();
			if(numOfRunningMapTasks < leastNumOfMapTasks )
			{
				leastNumOfMapTasks = numOfRunningMapTasks;
				leastLoadedMapperIp = ipAddress;
			}
		}
		nodeIdToMRInfo.get(getNodeIdFromIp(leastLoadedMapperIp)).incrementNumOfMapTasks();
		return leastLoadedMapperIp;
	}
	
	
	/**
	 * Get MR info for this node.
	 * @param nodeId
	 * @return
	 */
	public SlaveNodeInfo getSlaveNodeMRInfo(int nodeId)
	{
		return nodeIdToMRInfo.get(nodeId);
	}
	
	/**
	 * Get the cached connection to the node on the network.
	 * @param nodeId
	 * @return
	 */
	public HostInfo getSlaveHostConnectionInfo(int nodeId)
	{
		return nodeIdToNodeInfo.get(nodeId);
	}
	
	/**
	 * Assign a nodeId to a newly registered node, and store its connection info
	 * @param hostInfo
	 * @return
	 */
	public int addAndCacheNodeConnection(HostInfo hostInfo)
	{
		nodeCounter++;
		nodeIdToNodeInfo.put(nodeCounter, hostInfo);
		
		SlaveNodeInfo slaveNodeInfo = new SlaveNodeInfo();
		slaveNodeInfo.setLastUpdateReceivedTimestamp(System.currentTimeMillis());
		slaveNodeInfo.setNodeState(SlaveNodeState.ALIVE);
		slaveNodeInfo.setNumberOfRunningMapTasks(0);
		slaveNodeInfo.setNumberOfRunningReduceTasks(0);
		
		nodeIdToMRInfo.put(nodeCounter, slaveNodeInfo);
		
		// also send message to SFS client, to add this host to its store.
		try 
		{
			
			SfsAddOrRemoveNodeMessage message = new SfsAddOrRemoveNodeMessage(hostInfo.getIpAddress() , hostInfo.getSfsPort());
			message.setType(SfsMessage.SfsMessageType.ADD_HOST);
			CommunicationManager.sendMessage(sfsMasterHostInfo, message);
		}
		catch (SfsException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return nodeCounter;
	}
	
	
	/**
	 * Returns available reducers required for this task. It makes this decision based on the location of the 
	 * outputs of the map tasks for each partition.
	 * @param reducerInputs 
	 * @param size
	 * @return
	 */
	public List<Integer> getAvailableReducers(Map<Integer, List<TaskOutput>> reducerInputs, int numOfReducers) {
		
		List<Integer> ret = new LinkedList<>();
		
		for(Integer partititon : reducerInputs.keySet() )
		{
			List<TaskOutput> outputFiles = reducerInputs.get(partititon);
			List<String> reducerIps = new ArrayList<>();
			
			
			// get the count of map tasks that were run on each node for this partition.
			final Map<String, Integer> mapCountsOnNode = new HashMap<String, Integer>();
			
			
			for(TaskOutput output : outputFiles)
			{
				if(mapCountsOnNode.containsKey(output.getIpAddress()))
				{
					int count = mapCountsOnNode.get(output.getIpAddress());
					count++;
					mapCountsOnNode.put(output.getIpAddress(), count);
				}
				else
				{
					reducerIps.add(output.getIpAddress());
					mapCountsOnNode.put(output.getIpAddress() , 1);
				}
			}
			
			// sort the mapper Ips in decreasing order 
			//based on the count of map task that were running on them.
			Collections.sort(reducerIps, new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return -1 * mapCountsOnNode.get(o1).compareTo(mapCountsOnNode.get(o2));
				}
			
			});
			
			
			String selectedReducer = reducerIps.get(0);
			
			for(String reducerIp : reducerIps)
			{
				if(isNodeAlive(getNodeIdFromIp(reducerIp)) && !ret.contains(getNodeIdFromIp(reducerIp)))
				{
					selectedReducer = reducerIp;
					break;
				}
			}
			
			ret.add(getNodeIdFromIp(selectedReducer));
			
		}
		
		return ret;
		
	}
	
	/**
	 * Returns the node id fro a particular host on the network.
	 * @param ipAddress
	 * @return
	 */
	public int getNodeIdFromIp(String ipAddress)
	{
		for(Map.Entry<Integer, HostInfo> entry : nodeIdToNodeInfo.entrySet())
		{
			HostInfo info = entry.getValue();
           // System.out.println(entry.getKey() + " " + entry.getValue().getIpAddress());
			if(info.getIpAddress().equals(ipAddress))
				return entry.getKey();
		}
		
		return -1;
	}
	
	/**
	 * Called in case of a 
	 * @param mapTask
	 * @return
	 * @throws SfsFileDoesNotExistException 
	 */
	public int getAlternateMapperForTask(MapTask mapTask) throws SfsFileDoesNotExistException {
		
		String currentMapperIpAddress = nodeIdToNodeInfo.get(mapTask.getNodeId()).getIpAddress();
		InputSplit inputSplit = mapTask.getInputSplit();
		
		SfsFileInfo fileInfo = Sfs.getFileInfo(inputSplit.getActualFilePath());
		
		List<SfsSegmentInfo> segmentInfos = fileInfo.getSegmentPaths();
		
		int segmentNumber = -1;
		
		for(SfsSegmentInfo segmentInfo : segmentInfos)
		{
			if(segmentInfo.getFormalFilePath().toString().equals(inputSplit.getActualFilePath()))
				segmentNumber = segmentInfo.getSegmentNumber();
		}
		
		LinkedHashSet<String> hostsForThisChunk = fileInfo.getSegmentToHostListMap().get(segmentNumber);
		
		for(String newMapper : hostsForThisChunk)
		{
			// return the first mapper that is not the current one.
			if(!newMapper.equals(currentMapperIpAddress))
				return getNodeIdFromIp(newMapper);
		}
		
		
		return -1;
	}
	
	/**
	 * Receives heartbeat information from the worker nodes.
	 * @author surajd
	 *
	 */
	class HeartBeatManager implements Runnable
	{
		private ServerSocket serverSocket;
		
		public HeartBeatManager(int heartBeatPort) throws IOException
		{
			serverSocket = new ServerSocket(heartBeatPort);
		}

		@Override
		public void run() 
		{
			
			while(true)
			{
				try 
				{
					//accept connection.
					Socket socket = serverSocket.accept();
					
					// ip address of the sending node
					String workerNodeIp = socket.getInetAddress().getHostAddress();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					
					// read heartbeat message.
					HeartbeatMsg msg = (HeartbeatMsg) inputStream.readObject();
					// acknowledge heartbeat.
					outputStream.writeObject(Util.getAckMessage());
					
					int nodeId = getNodeIdFromIp(workerNodeIp);
					
					if(nodeId != -1)
					{
						// update last update received timestamp.
						nodeIdToMRInfo.get(nodeId).setLastUpdateReceivedTimestamp(System.currentTimeMillis());		
					}
					else
					{
						System.out.println(String.format("Unregisted worker with ip %s" , workerNodeIp));						
					}
					
				}
				catch (IOException | ClassNotFoundException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
	}
	
	/**
	 * Print all the node states.
	 */
	public void printNodeStates()
	{
		for(Integer nodeId : nodeIdToMRInfo.keySet())
		{
			System.out.println("Node with ip address " + nodeIdToNodeInfo.get(nodeId).getIpAddress() + " " +
					nodeId + " has state " + nodeIdToMRInfo.get(nodeId).getNodeState().name());
		}
	}
	
	public void incrementNumOfMapTasksOnNode(int nodeId)
	{
		nodeIdToMRInfo.get(nodeId).incrementNumOfMapTasks();
	}
	
	public void incrementNumOfReduceTasksOnNode(int nodeId)
	{
		nodeIdToMRInfo.get(nodeId).incrementNumOfReduceTasks();
	}
	
	public void decrementNumOfMapTasksOnNode(int nodeId)
	{
		nodeIdToMRInfo.get(nodeId).decrementNumOfMapTasks();
	}
	
	public void decrementNumOfReduceTasksOnNode(int nodeId)
	{
		nodeIdToMRInfo.get(nodeId).decrementNumOfReduceTasks();
	}
	

	public int getNewReducerForTask(ReduceTask reduceTask) {
		
		String currentReducerIpAddress = nodeIdToNodeInfo.get(reduceTask.getNodeId()).getIpAddress();
		
		for(Integer newReducerId : nodeIdToNodeInfo.keySet())
		{
			if(!nodeIdToNodeInfo.get(newReducerId).getIpAddress().equals(currentReducerIpAddress))
				return newReducerId;
		}
		
		return -1;
	}

	public int getTotalNumOfNodes() {
		return nodeIdToNodeInfo.size();
	}
		
	public boolean isNodeAlive(int nodeId)
	{
		return nodeIdToMRInfo.get(nodeId).getNodeState().equals(SlaveNodeState.ALIVE);
	}
	

}
