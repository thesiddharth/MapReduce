package mapreduce.dfs;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.*;

import mapreduce.dfs.data.*;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 10:39 PM
 * To change this template use File | Settings | File Templates.
 */

//Worker thread that will be running on data nodes.
//Responds to messages from the master to create/remove files and directories.
//To do this, it also has code to pull from another node's replica management thread.

public class SfsWorker implements Runnable
{
        private ObjectInputStream inputStream;
        private ObjectOutputStream outputStream;
        private ServerSocket workerSocket;
        //A reference to the local Sfs Tree. Passed in by Sfs.
        private SfsTree sfsTree;

        public SfsWorker(int workerPort, SfsTree sfsTree) throws Exception
        {
            workerSocket = new ServerSocket(workerPort);

            this.sfsTree = sfsTree;
        }

        @Override
        public void run()
        {

            while (true)
            {
                try
                {
                    Socket socket = workerSocket.accept();

                    outputStream = new ObjectOutputStream(socket.getOutputStream());
                    inputStream = new ObjectInputStream(socket.getInputStream());

                    System.out.println("Connection established");

                    SfsMessage message = (SfsMessage) inputStream.readObject();

                    if(message != null)
                    {

                        // process this message.
                        SfsMessage.SfsMessageType messageType = message.getType();

                        if(messageType == null)
                        {
                            throw new IllegalArgumentException("No message type is set.");
                        }

                        // check if this is a valid message type.
                        if(messageType.equals(SfsMessage.SfsMessageType.CREATE_DIRECTORY))
                        {
                            SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                            Sfs.correctPaths(updateFileInfo);

                            //Non-message sending protected invocation
                            Sfs._createDirectories(updateFileInfo.getFilePath());
                        }
                        else if(messageType.equals(SfsMessage.SfsMessageType.REMOVE_DIRECTORY))
                        {
                            SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                            Sfs.correctPaths(updateFileInfo);

                            //Non-message sending protected invocation
                            Sfs._deleteIfExists(updateFileInfo.getFilePath());
                        }
                        else if(messageType.equals(SfsMessage.SfsMessageType.CREATE_FILE))
                        {
                            SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                            Sfs.correctPaths(updateFileInfo);

                            System.out.println("Adding file");
                            Sfs.deleteIfExists(updateFileInfo.getFilePath());
                            //Add file information to tree.
                            sfsTree.addFileToTree(updateFileInfo);

                            //Need to pull in replicas?
                            if(((SfsCreationMessage) message).replicate())
                            {
                                //Put together a hashmap of the host to pull from, to the segments needed from it.
                                Set<Integer> segmentsToReplicate = ((SfsCreationMessage) message).getSegmentsToReplicate();

                                HashMap<String, List<SfsSegmentInfo>> hostToPullSegmentInfoMap = new HashMap<>();
                                List<SfsSegmentInfo> tempSegmentInfoList  = new ArrayList<>();

                                for(int segmentNumber : segmentsToReplicate)
                                    tempSegmentInfoList.add(updateFileInfo.getSegmentPaths().get(segmentNumber));

                                Iterator<String> iter = updateFileInfo.getHostListForSegment(0).iterator();

                                hostToPullSegmentInfoMap.put(iter.next(), tempSegmentInfoList);

                                pullReplicasFromHost(hostToPullSegmentInfoMap);
                            }
                            else
                            {
                                //Don't need to pull in replicas.
                                System.out.println("Not creating replica");
                            }
                        }
                        else if(messageType.equals(SfsMessage.SfsMessageType.REMOVE_FILE))
                        {
                            SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                            Sfs.correctPaths(updateFileInfo);

                            Sfs._deleteIfExists(updateFileInfo.getFilePath());
                        }
                        else if(messageType.equals(SfsMessage.SfsMessageType.UPDATE_INFO))
                        {
                            //Update file information for a file.
                            SfsFileInfo updateFileInfo = ((SfsUpdateMessage)message).getUpdatedFileInfo();
                            Sfs.correctPaths(updateFileInfo);

                            sfsTree.updateFile(updateFileInfo);
                        }
                        else if(messageType.equals(SfsMessage.SfsMessageType.RECONSTRUCT_TREE))
                        {
                            //Reconstruct the local tree using the root node info sent over the network
                            this.sfsTree.setRootNode(((SfsTreeTransferMessage)message).getRootNode());
                            Sfs.correctPaths(this.sfsTree);
                        }
                        else if(messageType.equals(SfsMessage.SfsMessageType.ENFORCE_REPLICAS))
                        {
                            //Enforce replication factor message.
                            HashMap<SfsSegmentInfo,String> incomingSegments = ((SfsReplicationEnforcementMessage)message).getSegmentInfoToUpdate();
                            //Has a node died?
                            String ipToRemove = ((SfsReplicationEnforcementMessage)message).getIpToRemove();
                            //Put together a hashmap of hosts to pull from, to the replica segments to pull from them
                            HashMap<String, List<SfsSegmentInfo>> hostToPullSegmentInfoMap = new HashMap<>();

                            for(SfsSegmentInfo segmentInfo : incomingSegments.keySet())
                            {
                                Sfs.correctPaths(segmentInfo);

                                SfsFileInfo updatedParentFile = Sfs.getFileInfo(segmentInfo.getFormalFilePath());

                                //If a node has died, remove it from the relevant files' segment to host list maps.
                                if(ipToRemove!=null && !ipToRemove.isEmpty())
                                    updatedParentFile.getSegmentToHostListMap().get(segmentInfo.getSegmentNumber()).remove(ipToRemove);

                                String newLocationOfThisSegment = incomingSegments.get(segmentInfo);

                                if(newLocationOfThisSegment!=null && !newLocationOfThisSegment.isEmpty())
                                {
                                    //Does this node need to pull in replicas
                                    if(newLocationOfThisSegment.equals(InetAddress.getLocalHost().getHostAddress()))
                                    {
                                        //This node needs to pull in replicas
                                        //Get random host to pull from and add an entry to the hashmap being maintain
                                        String hostIP = getRandomHost(updatedParentFile.getHostListForSegment(segmentInfo.getSegmentNumber()));

                                        if(!hostToPullSegmentInfoMap.containsKey(hostIP))
                                        {
                                            List<SfsSegmentInfo> newListOfSegments = new ArrayList<>();
                                            newListOfSegments.add(segmentInfo);
                                            hostToPullSegmentInfoMap.put(hostIP, newListOfSegments);
                                        }
                                        else
                                        {
                                            hostToPullSegmentInfoMap.get(hostIP).add(segmentInfo);
                                        }
                                    }

                                    //Update the segment to host list map with the new replica host info.
                                    updatedParentFile.addHostToSegmentHostList(segmentInfo.getSegmentNumber(), newLocationOfThisSegment);
                                }

                            }

                            //Pull replicas
                            pullReplicasFromHost(hostToPullSegmentInfoMap);

                        }

                        //Send back an ack when done.
                        SfsMessage ackMessage = new SfsMessage();
                        ackMessage.setType(SfsMessage.SfsMessageType.ACK_MESSAGE);

                        outputStream.writeObject(ackMessage);

                        inputStream.close();
                        outputStream.close();

                    }

                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

    //Selects a host from a host list uniformly randomly.
    private String getRandomHost(Set<String> hosts)
    {
        Iterator<String> iter = hosts.iterator();
        int randomHostIndex = new Random().nextInt(hosts.size());


        while(iter.hasNext() && --randomHostIndex >= 0)
            iter.next();

        return iter.next();
    }

    //Given a map of hosts to replica segments needed from those hosts, iteratively pulls each in.
    private void pullReplicasFromHost(HashMap<String, List<SfsSegmentInfo>> hostToSegmentMap) throws IOException {

        System.out.println("Pulling: ");
        for(String replicaHostIP : hostToSegmentMap.keySet())
        {
            SfsCompactReplicaRequest replicaRequest = new SfsCompactReplicaRequest();

            //Create the replica request object for this host. Add all paths needed.
            System.out.print("From: " + replicaHostIP + ":\t");
            for(SfsSegmentInfo segmentInfo : hostToSegmentMap.get(replicaHostIP))
            {
                replicaRequest.addPath(segmentInfo.getSfsPath().toString());
                System.out.print(segmentInfo.getSfsPath().getTopLevelFileOrDirName() + "||");
            }

            System.out.println();

            //Send out the replica request object
            Socket replicaDownloadSocket = new Socket(replicaHostIP, Sfs.getReplicaTransferPort());
            ObjectOutputStream outputStream = new ObjectOutputStream(replicaDownloadSocket.getOutputStream());
            outputStream.writeObject(replicaRequest);
            outputStream.flush();

            int currentFileIndex = 0;
            SfsSegmentInfo currSegment = hostToSegmentMap.get(replicaHostIP).get(0);

            //Using the message file information, we can see what size is expected for each segment. Get the first
            //segment size.
            long currentRemainingFileSize = currSegment.getSize();

            //Open an output stream to the first segment.
            BufferedOutputStream fileOutputStream = new BufferedOutputStream(Files.newOutputStream(currSegment.getSfsPath().getActualPath()));
            byte[] buffer = new byte[replicaDownloadSocket.getReceiveBufferSize()];
            long total_bytes_read = 0;

            //Start reading the data
            BufferedInputStream in = new BufferedInputStream(replicaDownloadSocket.getInputStream());
            int amountRead = -1;
            while ((amountRead = in.read(buffer)) > 0) {
                total_bytes_read += amountRead;

                if(currentRemainingFileSize <= amountRead)
                {
                    //We may have pulled in information about the next segment(s) as well.
                    int buffer_start = 0;
                    do
                    {
                        //Complete the current file
                        fileOutputStream.write(buffer, buffer_start, (int)currentRemainingFileSize);
                        buffer_start += currentRemainingFileSize;
                        fileOutputStream.close();
                        amountRead-=currentRemainingFileSize;
                        currentFileIndex++;

                        //Check if all expected segments have been pulled. If they have, we're done
                        if(currentFileIndex >= hostToSegmentMap.get(replicaHostIP).size())
                            break;

                        //Else, Open an outputstream to the next segment and repeat.
                        currSegment = hostToSegmentMap.get(replicaHostIP).get(currentFileIndex);
                        fileOutputStream = new BufferedOutputStream(Files.newOutputStream(currSegment.getSfsPath().getActualPath()));
                        currentRemainingFileSize = currSegment.getSize();
                    }
                    while(currentRemainingFileSize <= amountRead);

                    //Sanity check
                    if(currentFileIndex >= hostToSegmentMap.get(replicaHostIP).size())
                        break;

                    if(amountRead!=0)
                    {
                        //Still some data left, to be put into the current segment. Append and iterate.
                        fileOutputStream.write(buffer, buffer_start, amountRead);
                        currentRemainingFileSize = currSegment.getSize() - amountRead;
                    }
                }
                else
                {
                    //Only information on the current segment has been pulled in. Append and iterate.
                    currentRemainingFileSize -= amountRead;
                    fileOutputStream.write(buffer,0,amountRead);
                }
            }

            System.out.println(total_bytes_read + " bytes read.");

            in.close();
            outputStream.close();
        }
    }
}
