package mapreduce.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Random;

import mapreduce.data.HostInfo;
import mapreduce.data.MapReduceException;
import mapreduce.input.InputFormat;
import mapreduce.input.TextInputFormat;
import comm.HeartbeatMsg;
import comm.Message;
import comm.Message.MESSAGE_TYPE;

public class Util {
	
	public static final String MASTER_IP_ADDRESS_KEY = "MasterIpAddress";
	public static final String MR_MASTER_LISTENING_PORT_KEY = "MR_MasterListeningPort";
	public static final String MAX_NUMBER_OF_MAP_TASKS = "MaxNumberOfMapTasks";
	public static final String MAX_NUMBER_OF_REDUCE_TASKS = "MaxNumberOfReduceTasks";
	public static final String MASTER_HEART_BEAT_PORT_KEY = "MasterHeartBeatPort";
	public static final String MAX_NUMBER_OF_MR_JOBS = "MaxNumberOfMRJobs";
    public static final String SFS_MASTER_LISTENING_PORT = "SfsMasterPort";
    public static final String SFS_ROOT_PATH = "SFSRoot";
    public static final String REPLICA_TRANSFER_PORT = "ReplicaTransferPort";
    public static final String SFS_WORKER_NODE_PORTs = "WorkerNodePort";
    public static final String SFS_WORKER_NODE_IPs = "WorkerNodeIPs";
    public static final String CHUNK_SIZE = "ChunkSizeInMB";
    public static final String REPLICATION_FACTOR = "ReplicationFactor";
    public static final String CONFIG_FILE_PATH = "./Config.ini";
    //public static final String CONFIG_FILE_PATH = "/Users/surajd/Desktop/workspace/DSLab3/src/mapreduce/Config.ini";
	
	public static HostInfo getMasterAddress()
	{
		HostInfo hostInfo = new HostInfo();
		hostInfo.setIpAddress((String)ConfigManager.getConfigValue(MASTER_IP_ADDRESS_KEY));
		hostInfo.setPort((int)ConfigManager.getConfigValue(MR_MASTER_LISTENING_PORT_KEY));
		
		return hostInfo;
	}
	
	public static Message getAckMessage()
	{
		Message message = new Message();
		message.setMessageType(MESSAGE_TYPE.ACK);
		return message;
	}
	
	public static HeartbeatMsg getHeartBeatMessage()
	{
		HeartbeatMsg msg = new HeartbeatMsg();
		msg.setMessageType(MESSAGE_TYPE.HEARTBEAT_MSG);
		return msg;
	}
	
	public static InputFormat getInputFormatInstanceFromParameters(Class inputFormatClass)
	{
		try 
		{
			Object inputFormatClassInstance = inputFormatClass.newInstance();
			
			if(! (inputFormatClassInstance instanceof InputFormat<?, ?>) )
					throw new IllegalArgumentException("Invalid/Unknown input format.");
			
			if(inputFormatClass.isAssignableFrom(TextInputFormat.class))
			{
				return TextInputFormat.class.newInstance();
			}
			
			return null;
		}
		catch (InstantiationException | IllegalAccessException e)
		{
			throw new IllegalArgumentException("Invalid input format." , e);
		} 
	}
	
	public static Message getMapTaskStatusMessage()
	{
		Message message = new Message();
		message.setMessageType(MESSAGE_TYPE.IS_MAP_TASK_FINISHED);
		return message;
	}
	
	public static Message getFailureMessage()
	{
		Message message = new Message();
		message.setMessageType(MESSAGE_TYPE.FAILED);
		return message;
	}
	
	public static Message getRegisterWithServerMessage(int sfsListeningPort)
	{
		Message message = new Message();
		message.setMessage((Integer)sfsListeningPort);
		message.setMessageType(MESSAGE_TYPE.REGISTER_WITH_SERVER);
		return message;
	}

    public static int getIntConfigValue(String configValueKey)
    {
        List<String> valueList =  (List<String>) ConfigManager.getConfigValue(configValueKey);
        return Integer.parseInt(valueList.get(0));
    }

    public static String getStringConfigValue(String configValueKey)
    {
        List<String> valueList =  (List<String>) ConfigManager.getConfigValue(configValueKey);
        return (valueList.get(0));
    }

    public static Double getDoubleConfigValue(String configValueKey)
    {
        List<String> valueList =  (List<String>) ConfigManager.getConfigValue(configValueKey);
        return Double.parseDouble(valueList.get(0));
    }
    
    public static String getUniqueFileName()
    {
    	Random random = new Random();
    	return String.valueOf(random.nextInt(1000000));
    }
    
    public static Class<?> readClassFromJar(String localJarPath,  Class<?> baseClass , String className) throws MapReduceException
	{
		Class ret = null;
		
		try 
		{	
			URL[] urls = { new URL("jar:file:" + localJarPath+"!/") };
			URLClassLoader classLoader = new URLClassLoader(urls);
			
			ret = classLoader.loadClass(className);
			
			if(! (baseClass.isAssignableFrom(ret) ))
			{
				throw new MapReduceException("Required  implementation not found");
			}
			
		} 
		catch (MalformedURLException e)
		{
			throw new MapReduceException("Incorrect jar location", e);
		}
		catch (ClassNotFoundException e)
		{
			throw new MapReduceException(" class not found", e);
		} 
		catch (Exception e)
		{
			throw new MapReduceException(e);
		}
		
		return ret;
	}

    /**
     * Cleans up local resources before exiting a task.
     * @param directoryPath
     */
    public static void cleanupWorkingDirectory(String directoryPath)
    {
        Path directory = Paths.get(directoryPath);
        try
        {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
