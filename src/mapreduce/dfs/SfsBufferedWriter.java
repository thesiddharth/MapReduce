package mapreduce.dfs;

import mapreduce.dfs.data.SfsFileAlreadyExistsException;
import mapreduce.dfs.data.SfsMessage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/6/14
 * Time: 1:13 AM
 * To change this template use File | Settings | File Templates.
 */
// Wrapper over a buffered writer. All write functionality remains the same.
// The only change is for the close - files are split into chunks, the file info is populated with the
// relevant chunk segment info, and update messages are sent to the master, if suppress is not set.
public class SfsBufferedWriter
{
    private BufferedWriter writer;
    private SfsFileInfo fileInfo;
    private boolean suppressUpdateMessages;

    protected SfsBufferedWriter(BufferedWriter w, SfsFileInfo info)
    {
        writer = w;
        fileInfo = info;
        suppressUpdateMessages = false;
    }

    protected SfsBufferedWriter(BufferedWriter w, SfsFileInfo info, boolean suppressUpdateMessages)
    {
        writer = w;
        fileInfo = info;
        this.suppressUpdateMessages = suppressUpdateMessages;
    }

    //TODO Look into better exception handling
    public void close() throws IOException, SfsFileAlreadyExistsException {
        try
        {
            //Close underlying writer.
            writer.close();
            //Set closed
            fileInfo.setClosed(true);
            fileInfo.setSize(Files.size(fileInfo.getFilePath().getActualPath()));
            //Split into chunks
            List<SfsSegmentInfo> paths = splitAndStore(fileInfo.getFilePath());
            //Set list of chunk segments
            fileInfo.setSegmentPaths(paths);
            String localIP = Sfs.getLocalIP();
            //Update the segment to host info map each file info object maintains
            //with the local IP.
            for(int i = 0; i < paths.size(); i++)
            {
                LinkedHashSet<String> list = new LinkedHashSet<>();
                list.add(localIP);
                fileInfo.addSegmentToHostListMapEntry(i, list);
            }
            //TODO New thread?
            System.out.println("Sending create file message");
            // IF not suppressed, send over the update message to the master.
            if(!suppressUpdateMessages)
                Sfs.sendUpdateMessage(fileInfo, SfsMessage.SfsMessageType.CREATE_FILE);
        }
        catch (FileAlreadyExistsException e)
        {
            throw new SfsFileAlreadyExistsException(new Exception(new StringBuilder().append(fileInfo.getFilePath()).append(" already exists").toString()));
        }
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public void newline() throws IOException {
        writer.newLine();
    }

    public void write(int c) throws IOException {
        writer.write(c);
    }

    public void write(String s, int off, int len) throws IOException {
        writer.write(s, off, len);
    }

    public void write (char[] cbuf, int off, int len) throws IOException {
        writer.write(cbuf, off, len);
    }

    public void write(String s) throws IOException
    {
        writer.write(s);
    }

    //Returns the list of chunk segments the file has been divided into
    protected List<SfsSegmentInfo> splitAndStore(SfsPath path) throws IOException
    {
        Path actualPath = path.getActualPath();
        //TextInputFormat tif = new TextInputFormat(actualPath.toString());

        //Chunk selecting logic abstracted by the file chunk reader.
        SfsFileChunkReader reader = new SfsFileChunkReader(actualPath);

        int splitNumber = 0;
        ArrayList<SfsSegmentInfo> listOfSplitPaths = new ArrayList<>();

        //IS there another chunk?
        while(reader.hasNextChunk())
        {
            Path splitPath = Paths.get(actualPath.toString() + ".part" + splitNumber);
            BufferedWriter writer = Files.newBufferedWriter(splitPath, Charset.defaultCharset());
            long numRecordsInThisSplit = 0;

            //Is there another line in the current chunks
            while(reader.hasNextEntry())
            {
                writer.append(reader.getNextEntry()).append("\n");
                numRecordsInThisSplit++;
            }
            
            writer.close();

            listOfSplitPaths.add(new SfsSegmentInfo(SfsPath.getSfsPath(splitPath, Sfs.getHomePath()), Files.size(splitPath), path, numRecordsInThisSplit,splitNumber++));
        }

        reader.close();

        //Delete the original, un-split file.
        Files.delete(actualPath);
        return listOfSplitPaths;
    }
}
