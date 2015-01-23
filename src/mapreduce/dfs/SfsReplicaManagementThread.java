package mapreduce.dfs;

import mapreduce.dfs.data.SfsCompactReplicaRequest;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/10/14
 * Time: 5:04 PM
 * To change this template use File | Settings | File Templates.
 */

//A replica server thread.
//All data nodes will have this running. It listens for file requests, and spawns a new thread to handle each
//new request.
//For production level deployments, can be replaced with a non blocking socketchannel server very easily.
public class SfsReplicaManagementThread implements Runnable {

    private ServerSocket serverSocket;
    private int totalActiveSessions;
    private static final int RETRY_COUNT = 5;

    protected SfsReplicaManagementThread(int serverPort) throws IOException
    {
        this.serverSocket = new ServerSocket(serverPort);
        totalActiveSessions = 0;
    }


    @Override
    public void run() {
        try
        {
            final Socket socket = serverSocket.accept();

            new Thread(new Runnable() {
                @Override
                public void run() {

                    try{
                        final BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
                        final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                        System.out.println("Registering Replica Downloader: " + socket.getInetAddress().getHostAddress() + ", Total active sessions=" +  ++totalActiveSessions);

                        //Get a replica request object
                        SfsCompactReplicaRequest replicaRequest = (SfsCompactReplicaRequest) inputStream.readObject();

                        long total_bytes_written = 0;
                        long bytes_written;
                        Random randomNumber = new Random();
                        int i;
                        boolean errorFlag = false;

                        //Iterate over requested paths encapsulated in request object
                        for(String path: replicaRequest.getPaths())
                        {
                            for(i = 0 ; i < RETRY_COUNT; i++)
                            {
                                if(Sfs.isSfsPath(path))
                                {
                                    //File requested is an SFS file
                                    SfsPath sfsPath = Sfs.getPath(path);
                                    bytes_written = writeFile(sfsPath, outputStream);
                                    if(bytes_written < Files.size(sfsPath.getActualPath()))
                                    {
                                        //Error. Retry with exponential backoff.
                                        System.out.println("Error encountered. Retrying with exponential backoff.");
                                        Thread.sleep((1 << i) * 1000 + randomNumber.nextInt(1001));
                                        continue;
                                    }
                                    else
                                    {
                                        total_bytes_written += bytes_written;
                                        break;
                                    }
                                }
                                else
                                {
                                    //File requested is a local file, not on SFS.
                                    Path localPath = Paths.get(path);
                                    bytes_written = writeLocalFile(localPath, outputStream);
                                    if(bytes_written < Files.size(localPath))
                                    {
                                        System.out.println("Error encountered. Retrying with exponential backoff.");
                                        Thread.sleep((1 << i) * 1000 + randomNumber.nextInt(1001));
                                        continue;
                                    }
                                    else
                                    {
                                        total_bytes_written += bytes_written;
                                        break;
                                    }
                                }
                            }

                            if(i >= RETRY_COUNT)
                            {
                                errorFlag = true;
                                break;
                            }
                        }

                        System.out.println(total_bytes_written + " bytes written.");
                        if(errorFlag == true)
                        {
                            System.out.println("Retrying did not help - ending current replication and deleting partial copies.");
                        }

                        outputStream.close();
                        inputStream.close();
                        --totalActiveSessions;
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }).start();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    //Write out an SFS file to the socket
    long writeFile(SfsPath path, BufferedOutputStream outputStream) {

        long total_bytes_written = 0;

        try
        {
            boolean flag = false;

            BufferedInputStream segmentFileToRead = new BufferedInputStream(new FileInputStream(path.getActualPath().toString()));

            byte[] buffer = new byte[Sfs.BUFFER_LENGTH];
            int count;
            while((count=segmentFileToRead.read(buffer))!=-1)
            {
                outputStream.write(buffer, 0, count);
                total_bytes_written+=count;
            }

            segmentFileToRead.close();

            return total_bytes_written;
        }
        catch (Exception e) {
            return total_bytes_written;
        }
    }

    //Write out a local file to the socket
    long writeLocalFile(Path localFilePath, BufferedOutputStream outputStream)
    {
        long total_bytes_written = 0;

        try
        {
            BufferedInputStream segmentFileToRead = new BufferedInputStream(new FileInputStream(localFilePath.toString()));

            byte[] buffer = new byte[Sfs.BUFFER_LENGTH];
            int count = -1;
            while((count=segmentFileToRead.read(buffer))!=-1)
            {
                outputStream.write(buffer, 0 , count);
                total_bytes_written += count;
            }

            return total_bytes_written;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return total_bytes_written;
        }
    }
}
