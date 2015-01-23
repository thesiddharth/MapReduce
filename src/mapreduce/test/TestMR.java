package mapreduce.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import mapreduce.clients.JobClient;
import mapreduce.data.HostInfo;
import mapreduce.data.IntWritable;
import mapreduce.data.JobConf;
import mapreduce.data.LongWritable;
import mapreduce.data.Text;
import mapreduce.dfs.Sfs;
import mapreduce.dfs.SfsBufferedReader;
import mapreduce.dfs.SfsBufferedWriter;
import mapreduce.dfs.SfsPath;
import mapreduce.dfs.CommunicationManager.CommunicationManager;
import mapreduce.dfs.data.SfsAddOrRemoveNodeMessage;
import mapreduce.dfs.data.SfsMessage;
import mapreduce.input.TextInputFormat;
import mapreduce.master.MasterNode;
import mapreduce.slave.SlaveNode;
import mapreduce.util.ConfigManager;
import mapreduce.util.Util;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/18/14
 * Time: 7:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestMR
{
        private static final String OPTIONS = "1) Create directory: mkdir <directory_path> \n" +
                "2) Create empty file: mkfile <sfs_file_path> \n" +
                "3) Write to a file: write <sfs_file_path> \n" +
            "4) Read the file: read  <sfs_file_path> \n" +
            "5) Copy file from normal path to sfs path: copy <sfs_target> <source>\n" +
            "6) Delete file, if it exists: del <sfs_path>\n" +
            "7) Start WordCount : wc <numberOfReducers>\n" +
            "8) Start createAdjacencyList: adjList <numberOfReducers>" +
            "8) Exit : exit \n";
    private static final String lineBreak = "********************************************************";
    private static final String MASTER_OPTIONS = "1) Add node: add <HostIP> <ListeningPort>\n" +
            "2) Remove node remove <HostIP> <listeningPort>\n" +
            "3) Start WordCount : wc <numberOfReducers>\n" +
            "4) Start createAdjacencyList: adjList <numberOfReducers>" +
            "5) Exit : exit \n";


    public static void main(String args[])
    {
        try
        {
            Sfs.initialize();

            System.out.println(lineBreak);
            System.out.println("SFS up and running. You can do the following: ");
            boolean isMaster = false;
            List<String> valueList =  (List<String>) ConfigManager.getConfigValue(Util.MASTER_IP_ADDRESS_KEY);
            String masterIP = valueList.get(0);
            HostInfo thisHostInfo = new HostInfo();
            if(masterIP.equals(InetAddress.getLocalHost().getHostAddress()))
            {
                System.out.println(MASTER_OPTIONS);
                isMaster = true;
                thisHostInfo.setPort(1554);
                thisHostInfo.setIpAddress(InetAddress.getLocalHost().getHostAddress());
                MasterNode masterNode = new MasterNode(Util.CONFIG_FILE_PATH);
            }
            else
            {
                System.out.println(OPTIONS);
                SlaveNode slaveNode = new SlaveNode(Util.CONFIG_FILE_PATH);
            }
            System.out.println(lineBreak);

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;

            while(!(line = reader.readLine()).equalsIgnoreCase("exit"))
            {
                try
                {
                    String[] cmd = line.split("\\s");

                    switch(cmd[0])
                    {
                        case "mkdir" :
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            Sfs.createDirectories(Sfs.getPath(cmd[1]));
                            System.out.println("Created!");
                            break;

                        case "mkfile" :
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            Sfs.createFile(Sfs.getPath(cmd[1]), true);
                            System.out.println("Created!");
                            break;

                        case "write" :
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            SfsPath filePath = Sfs.getPath(cmd[1]);
                            System.out.println("Enter text. End with <end>");
                            SfsBufferedWriter writer = Sfs.newBufferedWriter(filePath, Charset.defaultCharset());
                            while(!(line = reader.readLine()).equals("<end>"))
                            {
                                writer.write(line+'\n');
                            }
                            writer.close();
                            break;

                        case "read" :
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            SfsPath readFilePath = Sfs.getPath(cmd[1]);
                            if(!Sfs.exists(readFilePath))
                            {
                                System.out.println("File doesn't exist");
                                break;
                            }
                            System.out.println("Reading line by line. Type <end> to end");
                            SfsBufferedReader reader1 = Sfs.newBufferedReader(readFilePath, Charset.defaultCharset());
                            String lineToPrint;
                            while((lineToPrint=reader1.readLine())!=null && !(reader.readLine()).equals("<end>"))
                            {
                                System.out.println(lineToPrint);
                            }
                            reader1.close();
                            break;

                        case "copy" :
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            Path source = Paths.get(cmd[2]);
                            SfsPath target = Sfs.getPath(cmd[1]);
                            Sfs.copy(source,target, StandardCopyOption.REPLACE_EXISTING);
                            System.out.println("Copied to SFS!");
                            break;

                        case "del" :
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            SfsPath delTarget = Sfs.getPath(cmd[1]);
                            Sfs.deleteIfExists(delTarget);
                            System.out.println("Deleted from SFS!");
                            break;

                        case "add":
                            if(!isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            SfsAddOrRemoveNodeMessage message = new SfsAddOrRemoveNodeMessage(cmd[1], Integer.parseInt(cmd[2]));
                            message.setType(SfsMessage.SfsMessageType.ADD_HOST);
                            CommunicationManager.sendMessage(thisHostInfo, message);
                            break;

                        case "remove" :
                            if(!isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            SfsAddOrRemoveNodeMessage removeMessage = new SfsAddOrRemoveNodeMessage(cmd[1], Integer.parseInt(cmd[2]));
                            removeMessage.setType(SfsMessage.SfsMessageType.REMOVE_HOST);
                            CommunicationManager.sendMessage(thisHostInfo, removeMessage);
                            break;

                        case "wc":
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            runWordCount(Integer.parseInt(cmd[1]));
                            break;
                        case "adjList":
                            if(isMaster)
                            {
                                System.err.println("Invalid command");
                                continue;
                            }
                            runCreateGraph(Integer.parseInt(cmd[1]));
                            break;
                        default:
                            System.out.println("Invalid command");
                            if(isMaster)
                                System.out.println(MASTER_OPTIONS);
                            else
                                System.out.println(OPTIONS);
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public static void runWordCount(int numReducers) throws Exception
    {
        SfsPath dir = Sfs.getPath("sfs://job1/input/");

		if(!Sfs.exists(dir))
			Sfs.createDirectories((dir));

		SfsPath file = Sfs.getPath("sfs://job1/input/Patents.txt");

		Path acutalFile = Paths.get("../Data/Patents.txt");
		//Path acutalFile = Paths.get("/Users/surajd/Desktop/4300.txt");

		Sfs.copy(acutalFile, file, StandardCopyOption.REPLACE_EXISTING);

		SfsPath dir1 = Sfs.getPath("sfs://job1/output/");

		if(!Sfs.exists(dir1))
			Sfs.createDirectories((dir1));


		JobConf job = new JobConf();

		job.setMapper(WordCount.Map.class);
		job.setReducer(WordCount.Reduce.class);

		job.setJarName("WordCount");
		job.setJarPath("../Data/WordCount.jar");
		//job.setJarPath("/Users/surajd/Desktop/WC.jar");
		job.setMapInputKey(LongWritable.class);
		job.setMapInputValue(Text.class);
	
		job.setReduceInputKey(Text.class);
		job.setReduceInputValue(IntWritable.class);
		
		job.setReduceOutputKey(Text.class);
		job.setReduceOutputValue(IntWritable.class);


		//job.setInputPaths(new String[]{"/Users/surajd/Desktop/4300.txt"});
		job.setInputPaths(new String[]{"sfs://job1/input/Patents.txt"});
		job.setOutputPath("../Output/job1/");
		//job.setOutputPath("/Users/surajd/Desktop/4300_outp/");

		job.setInputFormat(TextInputFormat.class);
		//job.setRecordSize(50);

		job.setNumberOfReducers(numReducers);

		JobClient.runJob(job);
    }
    
    public static void runCreateGraph(int numReducers) throws Exception
    {
        SfsPath dir = Sfs.getPath("sfs://job2/input/");

		if(!Sfs.exists(dir))
			Sfs.createDirectories((dir));

		SfsPath file = Sfs.getPath("sfs://job2/input/Patents.txt");

		Path acutalFile = Paths.get("../Data/Patents.txt");
		//Path acutalFile = Paths.get("/Users/surajd/Desktop/data.txt");

		Sfs.copy(acutalFile, file, StandardCopyOption.REPLACE_EXISTING);

		SfsPath dir1 = Sfs.getPath("sfs://job2/output/");

		if(!Sfs.exists(dir1))
			Sfs.createDirectories((dir1));


		JobConf job = new JobConf();

		job.setMapper(AdjListMap.class);
		job.setReducer(AdjListReduce.class);

		job.setJarName("CreateAdjList");
		job.setJarPath("../Data/AdjList.jar");
		//job.setJarPath("/Users/surajd/Desktop/AdjList.jar");
		

		job.setMapInputKey(LongWritable.class);
		job.setMapInputValue(Text.class);
	
		job.setReduceInputKey(Text.class);
		job.setReduceInputValue(Text.class);

		job.setReduceOutputKey(Text.class);
		job.setReduceOutputValue(Text.class);


		//job.setInputPaths(new String[]{"/Users/surajd/Desktop/data.txt"});
		job.setInputPaths(new String[]{"sfs://job2/input/Patents.txt"});
		job.setOutputPath("../Output/job2/");
		//job.setOutputPath("/Users/surajd/Desktop/4300_outp/");

		job.setInputFormat(TextInputFormat.class);

		job.setNumberOfReducers(numReducers);

		JobClient.runJob(job);
    }
}
