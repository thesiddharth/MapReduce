package mapreduce.dfs;

import mapreduce.dfs.CommunicationManager.CommunicationManager;
import mapreduce.data.HostInfo;
import mapreduce.dfs.data.*;
import mapreduce.util.ConfigManager;
import mapreduce.util.Util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 10:39 PM
 * To change this template use File | Settings | File Templates.
 */
//Sfs Master (NameNode) thread.
// Listens for file creation/deletion/updates from worker nodes and broadcasts update messages to maintain distributed
//      state.
// Keeps track of segments that still need to be replicated (in case there aren't enough worker(data) nodes)
//      as well as maintains a list of segments stored on each of the connected data nodes
// Also has the ability to add and remove datanode connections based on messages from the job tracker.
public class SfsMaster implements Runnable
{
        private ServerSocket serverSocket;
        // Map of chunk segments that still need to be replicated, but every node currently connected already has a copy
        // The value in the map is the number of additional replicas needed.
        // In case a new data node is added, these segments will be replicated there.
        private HashMap<SfsSegmentInfo, Integer> segmentsWithoutReplicas;
        // Map of data nodes to information about them.
        private ConcurrentHashMap<HostInfo, SfsWorkerNodeInformation> workerNodeToInformation;
        // Master's hierarchy tree
        private SfsTree sfsTree;
        // Master Host Info
        private HostInfo thisHostInfo;

        public SfsMaster (int servingPort, SfsTree sfsTree) throws Exception
        {
            serverSocket = new ServerSocket(servingPort);
            thisHostInfo = new HostInfo(InetAddress.getLocalHost().getHostAddress(), servingPort);
            workerNodeToInformation = new ConcurrentHashMap<>();

            //Read worker node IPs and port numbers from config file. Worker node information can be added statically
            //this way, or dynamically through an addition message.
            try
            {
                List<String> workerNodeIPList = (List<String>) ConfigManager.getConfigValue(Util.SFS_WORKER_NODE_IPs);
                List<String> workerNodePortsTemp = ((List<String>) ConfigManager.getConfigValue(Util.SFS_WORKER_NODE_PORTs));
                List<Integer> workerNodePortsFinal  = new ArrayList<>();

                for(String workerNodePort: workerNodePortsTemp)
                {
                    workerNodePortsFinal.add(Integer.parseInt(workerNodePort));
                }

                int count = 0;
                for(String ip: workerNodeIPList)
                {
                    workerNodeToInformation.put(new HostInfo(ip, workerNodePortsFinal.get(count++)), new SfsWorkerNodeInformation());
                }
            }
            catch (Exception e)
            {
                //Ignore - no config entries present
            }

            this.sfsTree = sfsTree;

            segmentsWithoutReplicas = new HashMap<>();
        }

        @Override
        public void run()
        {

            while (true)
            {
                try
                {
                    final Socket socket = serverSocket.accept();

                    //TODO
//                    if(!workerNodeToInformation.containsKey(new HostInfo(socket.getInetAddress().getHostAddress(), socket.getPort())))
//                    {
//                        System.err.println("Unknown Host");
//                        continue;
//                    }
                    System.out.println("Request from: " + socket.getInetAddress().getHostAddress() + "," + socket.getPort());

                    new Thread(new Runnable() {
                        @Override
                        public void run() {

                            try{
                                final ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                                final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                                System.out.println("Connection established");

                                //read incoming message.
                                final SfsMessage message = (SfsMessage) inputStream.readObject();

                                // process this message.
                                SfsMessage.SfsMessageType messageType = message.getType();

                                if(messageType == null)
                                {
                                    throw new IllegalArgumentException("No message type is set.");
                                }
                                // check if this is a valid message type.
                                if(messageType.equals(SfsMessage.SfsMessageType.CREATE_DIRECTORY))
                                {
                                    //
                                    SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                                    Sfs.correctPaths(updateFileInfo);

                                    System.out.println("Creating directory");

                                    //TODO Check
                                    SfsFileInfo update = new SfsFileInfo(updateFileInfo.getFilePath());
                                    sfsTree.addDirectoriesToTree(update);
//                                    Sfs._createDirectories();

                                    //Tell other nodes to add the directory
                                    broadcastUpdateMessages(updateFileInfo, message.getSenderHostInfo(), SfsMessage.SfsMessageType.CREATE_DIRECTORY);
                                }
                                else if(messageType.equals(SfsMessage.SfsMessageType.REMOVE_DIRECTORY))
                                {
                                    SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                                    Sfs.correctPaths(updateFileInfo);

                                    Sfs._deleteIfExists(updateFileInfo.getFilePath());

                                    //Tell other nodes to delete the directory
                                    broadcastUpdateMessages(updateFileInfo, message.getSenderHostInfo(), SfsMessage.SfsMessageType.REMOVE_DIRECTORY);
                                }
                                else if(messageType.equals(SfsMessage.SfsMessageType.CREATE_FILE))
                                {
                                    SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                                    Sfs.correctPaths(updateFileInfo);

                                    System.out.println("Creating file");

                                    //TODO
                                    if(Sfs.exists(updateFileInfo.getFilePath()))
                                    {
                                        sfsTree.updateFile(updateFileInfo);
                                    }
                                    else
                                    {
                                        sfsTree.addFileToTree(updateFileInfo);
                                    }

                                    // Update node information map
                                    try
                                    {
                                        workerNodeToInformation.get(message.getSenderHostInfo()).addToBytesUsed(updateFileInfo.getSize());
                                        workerNodeToInformation.get(message.getSenderHostInfo()).addStoredSegments(updateFileInfo.getSegmentPaths());
                                    }
                                    catch (NullPointerException e)
                                    {
                                        System.err.println("Bad request: Unknown Host " + message.getSenderHostInfo());
                                        return;
                                    }

                                    //Tell other nodes to update state and pull replicas as needed.
                                    broadcastCreateMessages(updateFileInfo, message.getSenderHostInfo(), SfsMessage.SfsMessageType.CREATE_FILE);
                                }
                                else if(messageType.equals(SfsMessage.SfsMessageType.REMOVE_FILE))
                                {
                                    SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                                    Sfs.correctPaths(updateFileInfo);

                                    //Remove all mentions of the relevant segments from the local worker information map
                                    for(HostInfo currHost: workerNodeToInformation.keySet())
                                    {
                                        Iterator<SfsSegmentInfo> segmentInfoIterator = workerNodeToInformation.get(currHost).getStoredSegments().iterator();
                                        while(segmentInfoIterator.hasNext())
                                        {
                                            SfsSegmentInfo currSegment = segmentInfoIterator.next();

                                            if(updateFileInfo.getSegmentPaths().contains(currSegment));
                                            {
                                                workerNodeToInformation.get(message.getSenderHostInfo()).addToBytesUsed(-currSegment.getSize());
                                                segmentInfoIterator.remove();
                                            }
                                        }
                                    }

                                    //Remove all mentions of the relevant segments from the yet-to-replicate list
                                    Iterator<SfsSegmentInfo> segmentsWithoutReplicasIterator = segmentsWithoutReplicas.keySet().iterator();

                                    while(segmentsWithoutReplicasIterator.hasNext())
                                    {
                                        if(updateFileInfo.getSegmentPaths().contains(segmentsWithoutReplicasIterator.next()))
                                            segmentsWithoutReplicasIterator.remove();
                                    }

                                    //Remove the file from the tree
                                    sfsTree.removeDirectoryOrFile(updateFileInfo.getFilePath());

                                    //Tell other nodes to remove the file too
                                    broadcastUpdateMessages(updateFileInfo, message.getSenderHostInfo(), SfsMessage.SfsMessageType.REMOVE_FILE);
                                }
                                else if(messageType.equals(SfsMessage.SfsMessageType.ADD_HOST))
                                {
                                    //Add a new data node when signalled by job tracker.
                                    SfsAddOrRemoveNodeMessage convertedMessage = (SfsAddOrRemoveNodeMessage)message;
                                    HostInfo nodeInfo = new HostInfo(convertedMessage.getIpAddress(), convertedMessage.getPortNo());
                                    addWorkerNodeInfo(nodeInfo);
                                    System.out.println("Node added: " + nodeInfo);
                                }
                                else if(messageType.equals(SfsMessage.SfsMessageType.REMOVE_HOST))
                                {
                                    // Remove an existing data node when signalled by job tracker
                                    SfsAddOrRemoveNodeMessage convertedMessage = (SfsAddOrRemoveNodeMessage)message;
                                    HostInfo nodeInfo = new HostInfo(convertedMessage.getIpAddress(), convertedMessage.getPortNo());
                                    if(workerNodeToInformation.containsKey(nodeInfo))
                                    {
                                        removeWorkerNodeInfo(nodeInfo);
                                        System.out.println("Node removed: " + nodeInfo);
                                    }
                                    else
                                    {
                                        System.err.println("Node doesn't exist.");
                                    }

                                }

                                //Send an acknowledgement back when done.
                                SfsAckMessage ackMessage = new SfsAckMessage(null);
                                ackMessage.setType(SfsMessage.SfsMessageType.ACK_MESSAGE);

                                outputStream.writeObject(ackMessage);

                                inputStream.close();
                                outputStream.close();
                            }
                            catch (IOException | ClassNotFoundException e)
                            {
                                e.printStackTrace();
                            }
                            catch (SfsException e)
                            {
                                //TODO
                                e.printStackTrace();
                            }
                        }
                    }).start();
                }
                catch (Exception e)
                {

                    e.printStackTrace();
                }
            }
        }

    //Send everyone but the sender an update message
    private void broadcastUpdateMessages(SfsFileInfo updatedFileInfo, HostInfo excludeHost, SfsMessage.SfsMessageType type) throws UnknownHostException, SfsException
    {

        HashSet<HostInfo> mailingList = new HashSet<>(workerNodeToInformation.keySet());

        mailingList.remove(excludeHost);

        SfsUpdateMessage message = new SfsUpdateMessage();
        message.setType(type);
        message.setUpdatedFileInfo(updatedFileInfo);
        message.setSenderHostInfo(thisHostInfo);

        for(HostInfo worker : mailingList)
        {
            CommunicationManager.sendMessage(worker, message);
        }
    }

    //Excluding the data node on which the file was created, considers other nodes in increasing order of space used.
    //Divides (replication factor * number of chunks in file) number of chunks as evenly as possible, starting with the
    //node with minimum space used.
    private void broadcastCreateMessages(SfsFileInfo updatedFileInfo, HostInfo senderHostInfo, SfsMessage.SfsMessageType type) throws UnknownHostException, SfsException
    {
        ArrayList<HostInfo> mailingList = new ArrayList<>(workerNodeToInformation.keySet());
        mailingList.remove(senderHostInfo);

        int replicationFactor = Sfs.getReplicationFactor();
        int numberOfSegmentsInFile = updatedFileInfo.getSegmentPaths().size();
        //Best case spread - each other node in the network gets only 1 chunk.
        int bestCaseReplicaSpread = numberOfSegmentsInFile * (replicationFactor-1);

        //Get nodes in increasing order of space used
        List<HostInfo> markedNodes = getNodesForReplication(mailingList, bestCaseReplicaSpread);
        mailingList.removeAll(markedNodes);

        HashMap<String, Set<Integer>> nodeToSegmentListMap = new HashMap<>();

        for(HostInfo hostInfo : markedNodes)
        {
            nodeToSegmentListMap.put(hostInfo.getIpAddress(), new HashSet<Integer>());
        }

        if(!markedNodes.isEmpty())
        {
            if(markedNodes.size()<bestCaseReplicaSpread)
            {
            // This means we can't have the 'each node gets only 1 chunk' case.
                if(markedNodes.size() < replicationFactor - 1)
                {
                // This means the replication factor cannot be enforced uniquely with the current set of nodes.
                    int difference = replicationFactor - 1 - markedNodes.size();
                    for(SfsSegmentInfo segmentInfo : updatedFileInfo.getSegmentPaths())
                    {
                        //Add the number of pending replicas to the global map.
                        segmentsWithoutReplicas.put(segmentInfo, difference);
                    }
                }

                // Minimum number of replicas each data node will get
                int minReplicaSegmentsToEach = bestCaseReplicaSpread/markedNodes.size();
                //Remaining replicas
                int remainingReplicaSegments = bestCaseReplicaSpread%markedNodes.size();

                int currReplicaSegment = 0;

                //Assign the min-to-each number of replicas calculated above to each data node
                //Maintain the worker node information map during these additions
                for(HostInfo hostInfo : markedNodes)
                {
                    Set<Integer> currNodeSegmentList = nodeToSegmentListMap.get(hostInfo.getIpAddress());
                    SfsWorkerNodeInformation currWorkerInformation = workerNodeToInformation.get(hostInfo);
                    for(int i = 0; i < minReplicaSegmentsToEach; i++)
                    {
                        currNodeSegmentList.add(currReplicaSegment);
                        updatedFileInfo.getSegmentToHostListMap().get(currReplicaSegment).add(hostInfo.getIpAddress());
                        currWorkerInformation.addToBytesUsed(updatedFileInfo.getSegmentPaths().get(currReplicaSegment).getSize());
                        currWorkerInformation.addStoredSegment(updatedFileInfo.getSegmentPaths().get(currReplicaSegment));
                        currReplicaSegment++;
                        if(currReplicaSegment == numberOfSegmentsInFile)
                            currReplicaSegment=0;
                    }
                }

                //Assign the remaining segments to nodes, again in increasing order of space used.
                //Maintain the worker node information map during these additions
                for(HostInfo hostInfo : markedNodes)
                {
                    if(remainingReplicaSegments == 0)
                        break;

                    remainingReplicaSegments--;

                    nodeToSegmentListMap.get(hostInfo.getIpAddress()).add(currReplicaSegment);
                    updatedFileInfo.getSegmentToHostListMap().get(currReplicaSegment).add(hostInfo.getIpAddress());
                    SfsWorkerNodeInformation currWorkerInformation = workerNodeToInformation.get(hostInfo);
                    currWorkerInformation.addToBytesUsed(updatedFileInfo.getSegmentPaths().get(currReplicaSegment).getSize());
                    currWorkerInformation.addStoredSegment(updatedFileInfo.getSegmentPaths().get(currReplicaSegment));
                    currReplicaSegment++;
                    if(currReplicaSegment == numberOfSegmentsInFile)
                        currReplicaSegment=0;
                }

            }
            else
            {
                //We can assign each chunk to an individual data node.
                int currReplicaSegment = 0;

                for(HostInfo hostInfo : markedNodes)
                {
                    Set<Integer> currNodeSegmentList = nodeToSegmentListMap.get(hostInfo.getIpAddress());
                    currNodeSegmentList.add(currReplicaSegment);
                    updatedFileInfo.getSegmentToHostListMap().get(currReplicaSegment).add(hostInfo.getIpAddress());
                    SfsWorkerNodeInformation currWorkerInformation = workerNodeToInformation.get(hostInfo);
                    currWorkerInformation.addToBytesUsed(updatedFileInfo.getSegmentPaths().get(currReplicaSegment).getSize());
                    currWorkerInformation.addStoredSegment(updatedFileInfo.getSegmentPaths().get(currReplicaSegment));
                    currReplicaSegment++;
                    if(currReplicaSegment == numberOfSegmentsInFile)
                        currReplicaSegment=0;
                }
            }
        }
        else
        {
            //No nodes to replicate on! Add everything to the pending list.
            int difference = replicationFactor - 1;
            for(SfsSegmentInfo segmentInfo : updatedFileInfo.getSegmentPaths())
            {
                segmentsWithoutReplicas.put(segmentInfo, difference);
            }
        }

        //Send out non-replicating messages.
        //This is for nodes that don't have to pull replicas, but hav to update their state.
        SfsCreationMessage message = new SfsCreationMessage();
        message.setType(type);
        message.setUpdatedFileInfo(updatedFileInfo);
        message.setSenderHostInfo(thisHostInfo);

        System.out.println("Sending nonreplicating messages to: ");
        System.out.println(mailingList.toString());

        for(HostInfo worker : mailingList)
        {
            CommunicationManager.sendMessage(worker, message);
        }

        //Send out replication messages - this is for nodes that have to pull replicas.
        System.out.println("Sending replicating messages to: ");
        System.out.println(markedNodes.toString());

        message.setReplicate(true);

        for(HostInfo worker : markedNodes)
        {
            message.setSegmentsToReplicate(nodeToSegmentListMap.get(worker.getIpAddress()));
            CommunicationManager.sendMessage(worker, message);
        }

        //Send back the updated file info (with completed list of replica hosts) to the node where
        //the file was originally created.
        message.setType(SfsMessage.SfsMessageType.UPDATE_INFO);
        message.setReplicate(false);
        message.setSegmentsToReplicate(null);

        CommunicationManager.sendMessage(senderHostInfo, message);
    }

    //Sort the nodes in increasing order of space used. Get a subset (Starting at 0) of this of desire size.
    private List<HostInfo> getNodesForReplication(ArrayList<HostInfo> candidates, int numberOfNodesNeeded)
    {
        ArrayList<HostInfo> markedNodes = new ArrayList<>(candidates);

        Collections.sort(markedNodes, increasingUsageComparator);

        int numberOfNodesToReturn = Math.min(numberOfNodesNeeded, markedNodes.size());

        return markedNodes.subList(0, numberOfNodesToReturn);
    }

    //Add a new data node
    private void addWorkerNodeInfo(HostInfo newNode) throws SfsException
    {
        //Put node into local node info map
        workerNodeToInformation.put(newNode, new SfsWorkerNodeInformation());

        //Send image of current file tree to the node
        SfsTreeTransferMessage message = new SfsTreeTransferMessage(sfsTree.getRootNode());
        message.setType(SfsMessage.SfsMessageType.RECONSTRUCT_TREE);
        CommunicationManager.sendMessage(newNode, message);

        //Any pending chunks/segments to replicate?
        if(!segmentsWithoutReplicas.isEmpty())
        {
            HashMap<SfsSegmentInfo, String> segmentToNewIpsList = new HashMap<>();

            //Assign a copy of each chunk/segment to replicate to the new node
            for(Map.Entry<SfsSegmentInfo, Integer> entry: segmentsWithoutReplicas.entrySet())
            {
                SfsSegmentInfo currSegment = entry.getKey();

                segmentToNewIpsList.put(currSegment, newNode.getIpAddress());
                workerNodeToInformation.get(newNode).addToBytesUsed(currSegment.getSize());
                workerNodeToInformation.get(newNode).addStoredSegment(currSegment);

                try
                {
                    SfsFileInfo updatedParentFile = Sfs.getFileInfo(currSegment.getFormalFilePath());
                    updatedParentFile.addHostToSegmentHostList(currSegment.getSegmentNumber(),newNode.getIpAddress());
                    entry.setValue(entry.getValue()-1);
                }
                catch (SfsFileDoesNotExistException e)
                {
                    e.printStackTrace();
                }


            }

            //Broadcast a message to all nodes.
            //New node will pull replicas.
            //Others will update their states
            SfsReplicationEnforcementMessage replicaMessage = new SfsReplicationEnforcementMessage();
            replicaMessage.setType(SfsMessage.SfsMessageType.ENFORCE_REPLICAS);
            replicaMessage.setSenderHostInfo(thisHostInfo);
            replicaMessage.setSegmentInfoToUpdate(segmentToNewIpsList);
            replicaMessage.setIpToRemove("");

            System.out.println("Sending replicating messages to: ");
            System.out.println(newNode.toString());

            for(HostInfo worker : workerNodeToInformation.keySet())
            {
                try {
                    CommunicationManager.sendMessage(worker, replicaMessage);
                } catch (SfsException e) {
                    //TODO
                    e.printStackTrace();
                }
            }
        }
    }

    //Remove a node
    private void removeWorkerNodeInfo(HostInfo deadNode)
    {
        Set<SfsSegmentInfo> segmentsToReplicate = workerNodeToInformation.get(deadNode).getStoredSegments();

        workerNodeToInformation.remove(deadNode);

        enforceReplicationFactorWhenNodeDies(segmentsToReplicate, deadNode);

    }

    //See if we can replicate the segments present on the dead node elsewhere.
    private void enforceReplicationFactorWhenNodeDies(Set<SfsSegmentInfo> segmentsToReplicate, HostInfo deadNode) {
        int numSegmentsToReplicate = segmentsToReplicate.size();
        List<HostInfo> candidates = getNodesForReplication(Collections.list(workerNodeToInformation.keys()), numSegmentsToReplicate);

        HashMap<SfsSegmentInfo, String> segmentToNewIpsList = new HashMap<>();

        //Remove the dead node's IP from all segments that resided on it.
        for(SfsSegmentInfo segmentInfo : segmentsToReplicate)
        {
            try
            {
                sfsTree.getFileInfo(segmentInfo.getFormalFilePath()).getSegmentToHostListMap().get(segmentInfo.getSegmentNumber()).remove(deadNode.getIpAddress());
            }
            catch (SfsFileDoesNotExistException e)
            {
                System.err.println("File " + segmentInfo.getFormalFilePath() + " not present");
                continue;
            }

            segmentToNewIpsList.put(segmentInfo, "");
        }

        //Try and get candidates for replication, again in increasing order of total space used
        if(!candidates.isEmpty())
        {
            Iterator<SfsSegmentInfo> segmentInfoIterator = segmentsToReplicate.iterator();

            while(segmentInfoIterator.hasNext())
            {
                SfsSegmentInfo currSegment = segmentInfoIterator.next();

                int i;

                for(i = 0; i < candidates.size() ; i++)
                {
                    if(!workerNodeToInformation.get(candidates.get(i)).getStoredSegments().contains(currSegment))
                        break;
                }

                if(i == candidates.size())
                {
                    //Nope, all candidates have this particular chunk already. Add it to the yet-to-replicate list.
                    System.out.println("All nodes contain: " + currSegment.getSfsPath().getTopLevelFileOrDirName());

                    if(!segmentsWithoutReplicas.containsKey(currSegment))
                        segmentsWithoutReplicas.put(currSegment,1);
                    else
                        segmentsWithoutReplicas.put(currSegment, segmentsWithoutReplicas.get(currSegment)+1);
                }
                else
                {
                    //Found a candidate without this segment.
                    //Tell candidate to pull the segment and update the relevant node's information
                    System.out.println(candidates.get(i) + "can get " + currSegment.getSfsPath().getTopLevelFileOrDirName());

                    segmentToNewIpsList.put(currSegment, candidates.get(i).getIpAddress());
                    workerNodeToInformation.get(candidates.get(i)).addToBytesUsed(currSegment.getSize());
                    workerNodeToInformation.get(candidates.get(i)).addStoredSegment(currSegment);

                    try
                    {
                        SfsFileInfo updatedParentFile = Sfs.getFileInfo(currSegment.getFormalFilePath());
                        updatedParentFile.addHostToSegmentHostList(currSegment.getSegmentNumber(), candidates.get(i).getIpAddress());
                    }
                    catch (SfsFileDoesNotExistException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }

        //SEnd out messages to everyone.
        //Nodes which have to pull replicas will do so.
        //Everyone will update state.
        SfsReplicationEnforcementMessage message = new SfsReplicationEnforcementMessage();
        message.setType(SfsMessage.SfsMessageType.ENFORCE_REPLICAS);
        message.setSenderHostInfo(thisHostInfo);
        message.setSegmentInfoToUpdate(segmentToNewIpsList);
        message.setIpToRemove(deadNode.getIpAddress());


        System.out.println("Sending replicating messages to: ");
        System.out.println(candidates.toString());

        for(HostInfo worker : workerNodeToInformation.keySet())
        {
            try {
                CommunicationManager.sendMessage(worker, message);
            } catch (SfsException e) {
                e.printStackTrace();
            }
        }
    }

    //Comparator for sorting data nodes
    Comparator<HostInfo> increasingUsageComparator = new Comparator<HostInfo>() {
        @Override
        public int compare(HostInfo o1, HostInfo o2) {

            long firstStorage = workerNodeToInformation.get(o1).getTotalUsedStorage();
            long secondStorage = workerNodeToInformation.get(o2).getTotalUsedStorage();

            if(firstStorage < secondStorage)
                return -1;
            else if(secondStorage > firstStorage)
                return 1;
            else
                return 0;
        }
    };

}
