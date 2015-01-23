package mapreduce.dfs;

import mapreduce.dfs.CommunicationManager.CommunicationManager;
import mapreduce.data.*;
import mapreduce.dfs.data.*;
import mapreduce.util.ConfigManager;
import mapreduce.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/4/14
 * Time: 1:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class Sfs
{
    //Will be set when Sfs is initialized
    private static boolean SFSExists = false;
    private static FileSystem fs;
    //Actual path for SFS file, abstracted from user.
    private static Path homePath;
    private static final String sfsPrefix = "sfs://";
    private static String fileSystemPath;
    //Port on which the replica management thread is running.
    private static int replicaTransferPort;
    //Size of Sfs File chunk in MB - Files will be divided into parts with this size
    private static double chunkSize;
    //How many copies of a file will be spread over the network, if possible.
    private static int replicationFactor;
    //Buffer length for file I/O
    public static final int BUFFER_LENGTH = 65536;
    //This node's IP
    private static String localIP;
    //This node's information, if worker
    private static HostInfo localWorkerHostInfo;
    //Master's information = this node's information, if this node is the master.
    private static HostInfo masterInfo;
    //Map representing overall filesystem view.
    private static SfsTree sfsTree;
    //Pattern to detect if file path represents an SFS split.
    private static Pattern splitPattern = Pattern.compile("(.+)\\.part\\d+");


    private Sfs()
    {}

    //Method to initialize SFS on the system
    public static void initialize() throws Exception
    {
       if(!SFSExists)
       {
           //Read config file
           ConfigManager.init(Util.CONFIG_FILE_PATH);

           String masterIP = Util.getStringConfigValue(Util.MASTER_IP_ADDRESS_KEY);

           int masterPort =  Util.getIntConfigValue(Util.SFS_MASTER_LISTENING_PORT);

           fileSystemPath = Util.getStringConfigValue(Util.SFS_ROOT_PATH);

           replicaTransferPort = Util.getIntConfigValue(Util.REPLICA_TRANSFER_PORT);

           chunkSize = Util.getDoubleConfigValue(Util.CHUNK_SIZE);

           replicationFactor = Util.getIntConfigValue(Util.REPLICATION_FACTOR);

           //Create home directory. Clean up the directory if it already exists.
           fs = FileSystems.getDefault();
           homePath = fs.getPath(fileSystemPath);
           if(!homePath.toFile().exists())
               new File(homePath.toString()).mkdir();
           else
               Util.cleanupWorkingDirectory(homePath.toString());

           //Master information
           masterInfo = new HostInfo(masterIP, masterPort);

           //Initialize the sfs tree with a home directory.
           SfsTreeNode rootNode = new SfsTreeNode(SfsTreeNodeType.DIRECTORY, new SfsFileInfo(new SfsPath(sfsPrefix, homePath)));
           sfsTree = new SfsTree(rootNode);

           try
           {
               //Is this host the master? If yes, start the master thread, if not, start the worker thread.
               localIP = InetAddress.getLocalHost().getHostAddress();
               if (masterIP.equals(localIP) )
               {
                   System.out.println("Starting the master thread");
                   new Thread(new SfsMaster(masterPort, sfsTree)).start();
                   //Can be added if needed. Not tested for this release, however.
//                   new Thread(new SfsWorker(1225, sfsTree)).start();
               }
               else
               {
                   System.out.println("Starting the worker thread");
                   //What port should I listen on?
                   int localWorkerPort = Integer.parseInt(((List<String>) ConfigManager.getConfigValue(Util.SFS_WORKER_NODE_PORTs)).get(0));
                   localWorkerHostInfo = new HostInfo(localIP, localWorkerPort);
                   new Thread(new SfsWorker(localWorkerPort, sfsTree)).start();
               }

               //Start the replica download thread. Every node has one of these running. Other nodes can send a message to the replica transfer port to get
               //any combination of local and sfs paths.
               Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new SfsReplicaManagementThread(replicaTransferPort), 0, 500, TimeUnit.MILLISECONDS);

               //SFS is now up.
               SFSExists = true;
           }
           catch (Exception e)
           {
               e.printStackTrace();
           }
       }
       else
       {
          throw new Exception("SFS already running!");
       }
    }

    //Create a directory
    public static SfsPath createDirectory(SfsPath path, FileAttribute<?> ... attrs) throws IOException
    {
        //Doesn't handle the case where file and directory have the same name

        //Return if it exists
        if(sfsTree.resolvePath(path) != null)
            return path;

        SfsPath returnValue = SfsPath.getSfsPath(Files.createDirectory(path.getActualPath(), attrs),homePath);

        SfsFileInfo update = new SfsFileInfo(path);

        //Make tree entry
        sfsTree.addDirectoryToTree(update);

        //Send a message to master so others can create the directory
        sendUpdateMessage(update, SfsMessage.SfsMessageType.CREATE_DIRECTORY);

        return returnValue;
    }

    //Copy from one SFS path to the other
    public static SfsPath copy(SfsPath source, SfsPath target, CopyOption... options) throws IOException, SfsFileAlreadyExistsException
    {

        try
        {
            if(source.getActualPath().toFile().isDirectory())
            {
                throw new IOException("Directory copying not supported");
            }
            else
            {
                //Is 'replace existing' a parameter?
                if(new HashSet<CopyOption>(Arrays.asList(options)).contains(StandardCopyOption.REPLACE_EXISTING))
                {
                    //Call create file with replace existing
                    createFile(target, true);
                }
                else
                {
                    createFile(target);
                }

                System.out.println("Copying ...");
                //Read the input file and copy to output file. SfsBufferedWriter and reader take care of lower level problems.
                SfsBufferedWriter writer = newBufferedWriter(target, Charset.defaultCharset());
                SfsBufferedReader reader = newBufferedReader(source, Charset.defaultCharset());
                char[] cbuf = new char[BUFFER_LENGTH];
                int readBytes = -1;

                while((readBytes=reader.read(cbuf))!=-1)
                    writer.write(cbuf,0,readBytes);

                writer.close();
                System.out.println("Copied");

            }

            return target;
        }
        catch(FileAlreadyExistsException e)
        {
            throw new SfsFileAlreadyExistsException(new Exception(new StringBuilder().append(target.toString()).append(" already eexists.").toString()));
        }
        catch (SfsFileDoesNotExistException | SfsFileClosedException e)
        {
            e.printStackTrace();
            throw new IOException();
        }
    }

    //Copy from normal filesystem path into SFS path
    public static SfsPath copy(Path source, SfsPath target, CopyOption... options) throws IOException, SfsFileAlreadyExistsException
    {
        try
        {
            if(source.toFile().isDirectory())
            {
                throw new IOException("Directory copying not supported");
            }
            else
            {
                //Is 'replace existing' a parameter?
                if(new HashSet<CopyOption>(Arrays.asList(options)).contains(StandardCopyOption.REPLACE_EXISTING))
                {
                    //Create file with replacement
                    createFile(target, true);
                }
                else
                {
                    createFile(target);
                }

                System.out.println("Copying ...");
                //Read the input file and copy to output file. SfsBufferedWriter and reader take care of lower level problems.
                SfsBufferedWriter writer = newBufferedWriter(target, Charset.defaultCharset());
                BufferedReader reader = new BufferedReader(new FileReader(source.toString()));
                char[] cbuf = new char[BUFFER_LENGTH];
                int readBytes = -1;

                while((readBytes=reader.read(cbuf))!=-1)
                    writer.write(cbuf,0,readBytes);

                writer.close();
                System.out.println("Copied");
            }

            return target;
        }
        catch(FileAlreadyExistsException e)
        {
            throw new SfsFileAlreadyExistsException(new Exception(new StringBuilder().append(target.toString()).append(" already eexists.").toString()));
        }
        catch (SfsFileDoesNotExistException| SfsFileClosedException e)
        {
            e.printStackTrace();
            throw new IOException();
        }
    }

    //Copy from SFS to normal filesystem path
    public static Path copy(SfsPath source, Path target, CopyOption... options) throws IOException, SfsFileDoesNotExistException
    {
        if(source.getActualPath().toFile().isDirectory())
        {
            throw new IOException("Directory copying not supported");
        }
        else
        {
            Files.createFile(target);

            System.out.println("Copying ...");
            BufferedWriter writer = Files.newBufferedWriter(target, Charset.defaultCharset());
            SfsBufferedReader reader = newBufferedReader(source, Charset.defaultCharset());
            char[] cbuf = new char[BUFFER_LENGTH];
            int readBytes = -1;

            while((readBytes=reader.read(cbuf))!=-1)
                writer.write(cbuf,0,readBytes);

            writer.close();
            System.out.println("Copied");
        }

        return target;
    }


    //Create all directories in the path specified.
    public static SfsPath createDirectories(SfsPath dir, FileAttribute<?>... attrs) throws IOException
    {
        if(sfsTree.resolvePath(dir) != null)
            return dir;

        SfsPath returnValue = SfsPath.getSfsPath(Files.createDirectories(dir.getActualPath(), attrs), homePath);

        SfsFileInfo update = new SfsFileInfo(dir);

        //Add all directories to the tree.
        sfsTree.addDirectoriesToTree(update);

        sendUpdateMessage(update, SfsMessage.SfsMessageType.CREATE_DIRECTORY);

        return returnValue;
    }

    // Create a file. Throws error if file exists.
    public static SfsPath createFile(SfsPath path) throws IOException, SfsFileAlreadyExistsException
    {
        try
        {

            if(sfsTree.resolvePath(path)!=null)
                throw new SfsFileAlreadyExistsException(new Exception(new StringBuilder().append(path).append(" already exists").toString()));

            System.out.println("Creating: " + path);

            SfsPath returnValue = SfsPath.getSfsPath(Files.createFile(path.getActualPath()), homePath);

            System.out.println("Created: " + path);

            if(returnValue != null)
            {
                sfsTree.addFileToTree(new SfsFileInfo(false, path));
            }
            else
            {
                throw new IOException();
            }

            //No updates sent here - updates are only sent when the file is written to.

            return path;
        }
        catch (FileAlreadyExistsException e)
        {
            throw new SfsFileAlreadyExistsException(new Exception(new StringBuilder().append(path).append(" already exists").toString()));
        }

    }

    // Create a file. Truncates the file if truncate is set and file already exists.
    public static SfsPath createFile(SfsPath path, boolean truncate) throws IOException, SfsFileAlreadyExistsException
    {
        if(truncate == true)
        {
            deleteIfExists(path);

            System.out.println("Creating: " + path);

            SfsPath returnValue = SfsPath.getSfsPath(Files.createFile(path.getActualPath()), homePath);

            System.out.println("Created: " + path);

            if(returnValue != null)
            {
                sfsTree.addFileToTree(new SfsFileInfo(false, path));
            }
            else
            {
                throw new IOException();
            }

            //No updates sent here - updates are only sent when the file is written to.

            return path;
        }
        else
        {
            return createFile(path);
        }
    }

    //Check if the specified empty directory or file exists. If it does, delete it.
    public static boolean deleteIfExists(SfsPath path) throws IOException
    {
        try
        {
            //If not mapped in tree
            if(sfsTree.resolvePath(path) == null)
            {
                //File may still exist from a previous run. Just to make sure, call Files.deleteIfExists.
                Files.deleteIfExists(path.getActualPath());
                return false;
            }

            //Get info from tree.
            SfsFileInfo fileInfo = sfsTree.getFileInfo(path);

            if(path.getActualPath().toFile().isDirectory())
            {
                //Delete empty directory. Send a message to the master so that other nodes can do this as well.
                sendUpdateMessage(Sfs.getFileInfo(path), SfsMessage.SfsMessageType.REMOVE_DIRECTORY);
                Files.deleteIfExists(path.getActualPath());
            }
            else
            {
                //Delete the file. Send a message to the master so that other nodes can do this as well.
                sendUpdateMessage(Sfs.getFileInfo(path), SfsMessage.SfsMessageType.REMOVE_FILE);
                for(SfsSegmentInfo segment : Sfs.getFileInfo(path).getSegmentPaths())
                {
                    Files.deleteIfExists(segment.getSfsPath().getActualPath());
                }
            }

            //Remove tree mapping.
            sfsTree.removeDirectoryOrFile(path);

            return true;
        }
        catch (SfsFileDoesNotExistException e)
        {
            return false;
        }
    }

    //Is this path a directory
    public static boolean isDirectory(SfsPath path, LinkOption... options) throws IOException
    {
        return Files.isDirectory(path.getActualPath(), options);
    }

    //IS this path readable
    public static boolean isReadable(SfsPath path) throws IOException
    {
        return Files.isReadable(path.getActualPath());
    }

    //Does this path exist in SFS
    public static boolean exists(SfsPath path) throws IOException
    {
        return sfsTree.resolvePath(path)!=null;
    }

    //Get a new buffered reader for SFS files or individual SFS chunk.
    //In the first case, it will loop over all chunks internally and provide a normal buffered reader view to the user. HOWEVER, this requires
    //                all the chunks to be present on the system. We are not handling pulling in files from other nodes into streams as of now.
    //In the second case, it checks if the path is a valid chunk path and returns a reader only for that chunk.
    public static SfsBufferedReader newBufferedReader(SfsPath path, Charset cs) throws IOException, SfsFileDoesNotExistException
    {
        if(sfsTree.resolvePath(path)==null)
        {
            //Does the path represent a particular chunk?
            Matcher matcher = splitPattern.matcher(path.toString());
            if(matcher.find() && sfsTree.resolvePath(matcher.group(1))!=null)
            {
                //Yes, return a buffered reader only for that chunk
                return new SfsBufferedReader(path, Charset.defaultCharset());
            }
            else
            {
                throw new SfsFileDoesNotExistException(new Exception(new StringBuilder().append(path).append(" does not exist").toString()));
            }
        }
        else
        {
            //Return a buffered reader for entire file
            return new SfsBufferedReader(Sfs.getFileInfo(path),cs);
        }
    }


    //Get a new buffered reader for SFS files or individual SFS chunk.
    //In the first case, it will loop over all chunks internally and provide a normal buffered reader view to the user. HOWEVER, this requires
    //                all the chunks to be present on the system. We are not handling pulling in files from other nodes into streams as of now.
    //In the second case, it checks if the path is a valid chunk path and returns a reader only for that chunk.
    public static SfsBufferedReader newBufferedReader(String path, Charset cs) throws IOException, SfsFileDoesNotExistException
    {
        //Check if a particular split is being read
        if(sfsTree.resolvePath(path)==null)
        {
            //Does the path represent a particular chunk?
            Matcher matcher = splitPattern.matcher(path);
            if(matcher.find() && sfsTree.resolvePath(matcher.group(1))!=null)
            {
                //Yes, return a buffered reader only for that chunk
                return new SfsBufferedReader(Sfs.getPath(path), Charset.defaultCharset());
            }
            else
            {
                throw new SfsFileDoesNotExistException(new Exception(new StringBuilder().append(path).append(" does not exist").toString()));
            }
        }
        else
        {
            //Return a buffered reader for entire file
            return new SfsBufferedReader(Sfs.getFileInfo(path),cs);
        }
    }

    //Returns an encapsulated buffered writer. When closed, splitting of the file into chunks is initiated, update messages are sent and the file is closed for further writes.
    public static SfsBufferedWriter newBufferedWriter(SfsPath path, Charset cs, OpenOption... options) throws SfsFileClosedException, IOException, SfsFileDoesNotExistException, SfsFileAlreadyExistsException {
        try {
            SfsFileInfo checkInfo = sfsTree.resolvePath(path);

            //If file does not exist and either no openoptions are passed or the options contain create - create a new file.
            if(checkInfo==null)
            {
                if((options.length==0 || new HashSet<OpenOption>(Arrays.asList(options)).contains(StandardOpenOption.CREATE)))
                {
                    createFile(path);
                    checkInfo = sfsTree.resolvePath(path);
                }
                else
                {
                    throw new SfsFileDoesNotExistException(new Exception(new StringBuilder().append(path.toString()).append(" does not exist").toString()));
                }
            }

            //File exists but is closed for write
            if(checkInfo!=null && checkInfo.isClosed())
                throw new SfsFileClosedException(new Exception("File is closed for write"));

            return new SfsBufferedWriter(Files.newBufferedWriter(path.getActualPath(), cs, options), checkInfo);
        }
        catch (FileNotFoundException e)
        {
            throw new SfsFileDoesNotExistException(new Exception(new StringBuilder().append(path.toString()).append(" does not exist").toString()));
        }
        catch (FileAlreadyExistsException e)
        {
            throw new SfsFileAlreadyExistsException(new Exception(new StringBuilder().append(path.toString()).append(" already exists").toString()));
        }
    }

    //Get size of input path.
    public static long size(SfsPath path) throws IOException
    {
        return Files.size(path.getActualPath());
    }

    //Lookup the tree for the file info stored
    public static SfsFileInfo getFileInfo(String filePath) throws SfsFileDoesNotExistException
    {
        return sfsTree.getFileInfo(filePath);
    }

    //Lookup the tree for file info stored
    public static SfsFileInfo getFileInfo(SfsPath filePath) throws SfsFileDoesNotExistException
    {
        return sfsTree.getFileInfo(filePath);
    }

    ///////////////////////////////////////////////////////////////////////////////
    /// PROTECTED METHODS FOR NO UPDATE CALLS
    // These will be called internally by SFS methods when something needs to
    // be created,removed or updated without update messages being sent out.
    // This is primarily needed when receiving messages from the master.
    ///////////////////////////////////////////////////////////////////////////////

    protected static SfsPath _createDirectory(SfsPath path, FileAttribute<?> ... attrs) throws IOException
    {

        if(sfsTree.resolvePath(path) != null)
            return path;

        SfsPath returnValue = SfsPath.getSfsPath(Files.createDirectory(path.getActualPath(), attrs),homePath);

        SfsFileInfo update = new SfsFileInfo(path);

        sfsTree.addDirectoryToTree(update);

        return returnValue;
    }

    protected static SfsPath _createDirectories(SfsPath dir, FileAttribute<?>... attrs) throws IOException
    {
        if(sfsTree.resolvePath(dir) != null)
            return dir;

        SfsPath returnValue = SfsPath.getSfsPath(Files.createDirectories(dir.getActualPath(), attrs), homePath);

        SfsFileInfo update = new SfsFileInfo(dir);

        sfsTree.addDirectoriesToTree(update);

        return returnValue;
    }

    protected static boolean _deleteIfExists(SfsPath path) throws IOException
    {
        try
        {
            if(sfsTree.resolvePath(path) == null)
            {
                //File may still exist from a previous run. Just to make sure, call Files.deleteIfExists.
                Files.deleteIfExists(path.getActualPath());
                return false;
            }

            SfsFileInfo fileInfo = sfsTree.getFileInfo(path);

            if(path.getActualPath().toFile().isDirectory())
            {
                Files.delete(path.getActualPath());
            }
            else
            {
                for(SfsSegmentInfo segment : Sfs.getFileInfo(path).getSegmentPaths())
                {
                    Files.deleteIfExists(segment.getSfsPath().getActualPath());
                }
            }

            sfsTree.removeDirectoryOrFile(path);

            return true;
        }
        catch (SfsFileDoesNotExistException e)
        {
            return false;
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////

    //Send a message to the master communicating the local update made
    protected static void sendUpdateMessage(SfsFileInfo info, SfsMessage.SfsMessageType type)
    {
        SfsUpdateMessage message;
        message = new SfsUpdateMessage();
        message.setUpdatedFileInfo(info);
        message.setType(type);
        message.setSenderHostInfo(localWorkerHostInfo);

        try
        {
            CommunicationManager.sendMessage(masterInfo, message);
        }
        catch (SfsException e)
        {
            //TODO
            e.printStackTrace();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    //// PATH RECONSTRUCTION METHODS
    //When a path is passed in to this machine, the underlying Filesystem paths are null
    //(transient objects as they are not serializable).
    //We need to reconstruct them.
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    //For File
    protected static void correctPaths(SfsFileInfo fileInfo)
    {
        fileInfo.getFilePath().setHomePath(homePath);
        fileInfo.getFilePath().setActualPath(Sfs.getPath(fileInfo.getFilePath().toString()).getActualPath());

        for(SfsSegmentInfo segmentInfo : fileInfo.getSegmentPaths())
        {
            correctPaths(segmentInfo);
        }
    }

    //For Chunk segments within a file
    protected static void correctPaths(SfsSegmentInfo segmentInfo)
    {
       SfsPath path = segmentInfo.getSfsPath();
       path.setHomePath(homePath);
       path.setActualPath(Sfs.getPath(path.toString()).getActualPath());
    }

    //For a whole tree
    protected static void correctPaths(SfsTree incomingTree)
    {
        correctPathsHelper(incomingTree.getRootNode());
    }

    //Recursive helper function for the tree
    protected static void correctPathsHelper(SfsTreeNode treeNode)
    {
        correctPaths(treeNode.getNodeInfo());
        for(SfsTreeNode child : treeNode.getChildren().values())
        {
            correctPathsHelper(child);
            if(child.getType().equals(SfsTreeNodeType.DIRECTORY))
            {
                try {
                    Files.createDirectories(child.getNodeInfo().getFilePath().getActualPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    //Is this path an sfs path?
    public static boolean isSfsPath(String path)
    {
        return  path.startsWith("sfs://");
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// GETTERS
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static int getReplicaTransferPort() {
        return replicaTransferPort;
    }

    //Only way of getting new SFS paths. Allows for sanitizing paths at one place, and right at the beginning.
    public static SfsPath getPath(String path) throws IllegalArgumentException
    {
        return new SfsPath(path,homePath);
    }

    protected static Path getHomePath() {
        return homePath;
    }

    public static String getLocalIP() {
        return localIP;
    }

    public static double getChunkSize() {
        return chunkSize;
    }

    public static int getReplicationFactor() {
        return replicationFactor;
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// MAIN FOR TESTING

    public static void main(String args[])
    {
        try
        {
            Sfs.initialize();
            System.out.println("SFS up and running: " + InetAddress.getLocalHost().getHostAddress());
//            Thread.sleep(15000);
////
//            Thread.sleep(20000);
//
//            SfsPath dir = Sfs.getPath("sfs://firstWorkingDirectory/Test1/");
//            System.out.println(dir);
//            SfsPath file = dir.resolve("FirstFile.txt");
//            if (!Sfs.exists(dir))
//            {
//                Sfs.createDirectories(dir);
//            }
//            SfsPath newPath = Sfs.createFile(file);
//            System.out.println(newPath.getTopLevelFileOrDirName());
//            SfsBufferedWriter br = Sfs.newBufferedWriter(file, Charset.defaultCharset(), StandardOpenOption.WRITE);
//            br.write("123LineInSFS\n234LineInSFS\n345LineInSFS\n456LineInSFS\n567LineInSFS\n678LineInSFS");
//            br.close();
//
//            Sfs.createDirectories(dir.resolve("NuTest/"));
//
//            SfsBufferedReader reader = Sfs.newBufferedReader(newPath, Charset.defaultCharset());
//            char[] buf = new char[10];
//            System.out.println(reader.read(buf));
//
//            System.out.println(buf);
//            reader.read(buf);
//
//            System.out.println(buf);
//            System.out.println((char)reader.read());
//            System.out.println(reader.readLine());
//
//            System.out.println(reader.readLine());
//
//            SfsFileInfo fileInfo = Sfs.getFileInfo(newPath);
//            SfsPath splitPath = fileInfo.getSegmentPaths().get(2).getSfsPath();
//            SfsBufferedReader splitReader = Sfs.newBufferedReader(splitPath,Charset.defaultCharset());
//            System.out.println(splitReader.readLine());
//            System.out.println(splitReader.readLine());
//
//            splitReader.close();
//            reader.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
