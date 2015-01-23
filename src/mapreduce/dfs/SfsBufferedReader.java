package mapreduce.dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/6/14
 * Time: 1:13 AM
 * To change this template use File | Settings | File Templates.
 */
// Wrapper over a normal buffered reader to read SFS files and chunks
// For a SFS file: it will iterate over chunks. However it assumes all chunks are located on the local machine.
//                 This can be extended to pulling in chunks from other machines directly to streams, so as to
//                 not affect the replication factor.
// For a chunk: Acts like a simple buffered reader over the chunk.
public class SfsBufferedReader
{
    private BufferedReader reader;
    private SfsFileInfo fileInfo;
    private SfsSegmentInfo currentSegment;
    private int maxSegments;
    private int currentSegmentIndex;
    private Charset cs;

    //Constructor for file reading.
    protected SfsBufferedReader(SfsFileInfo info, Charset cs) throws IOException
    {
        fileInfo = info;
        this.cs = cs;
        currentSegmentIndex = 0;
        maxSegments = info.getSegmentPaths().size();
        currentSegment = info.getSegmentPaths().get(currentSegmentIndex);
        reader = Files.newBufferedReader(currentSegment.getSfsPath().getActualPath(), cs);
    }

    //Constructor for only chunk reading.
    protected SfsBufferedReader(SfsPath splitFilePath, Charset cs) throws IOException
    {
        fileInfo = new SfsFileInfo(splitFilePath);
        this.cs = cs;
        currentSegmentIndex = 0;
        maxSegments = 1;
        currentSegment = new SfsSegmentInfo(splitFilePath, Files.size(splitFilePath.getActualPath())); // Won't actually be used
        reader = Files.newBufferedReader(splitFilePath.getActualPath(), cs);
    }


    public void close() throws IOException {
        reader.close();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    /// Read methods
    // If the underlying read method returns nothing - check if there's another split.
    // If there is, start reading from that, otherwise return null or -1, as the prototype
    // demands
    ///////////////////////////////////////////////////////////////////////////////////////////

    public int read() throws IOException {
        int returnValue = reader.read();
        if(returnValue == -1)
        {
            if(currentSegmentIndex == maxSegments - 1)
            {
                return -1;
            }
            else
            {
                update();
                return read();
            }
        }
        else
        {
            return returnValue;
        }
    }

    //Helper function
    private void update() throws IOException {
        currentSegmentIndex++;
        currentSegment = fileInfo.getSegmentPaths().get(currentSegmentIndex);
        reader = Files.newBufferedReader(currentSegment.getSfsPath().getActualPath(), cs);
    }

    //Helper function
    private int readHelper(CharBuffer charBuffer, int totalAmountRead) throws IOException
    {
        int returnValue = reader.read(charBuffer);
        if(returnValue > 0)
        {
            totalAmountRead += returnValue;

            if(charBuffer.remaining() == 0)
                return totalAmountRead;
            else
                return readHelper(charBuffer, totalAmountRead);
        }
        else if(returnValue == -1)
        {
            if(currentSegmentIndex == maxSegments - 1)
            {
                return totalAmountRead > 0 ? totalAmountRead : -1;
            }
            else
            {
                update();
                return readHelper(charBuffer, totalAmountRead);
            }
        }
        else
        {
            return 0;
        }
    }

    public int read(char[] cbuf) throws IOException {
        CharBuffer charBuffer = CharBuffer.wrap(cbuf);
        return readHelper(charBuffer, 0);
    }

    public String readLine() throws IOException {
        String returnValue = reader.readLine();
        if(returnValue == null)
        {
            if(currentSegmentIndex == maxSegments - 1)
            {
                return null;
            }
            else
            {
                update();
                return readLine();
            }
        }
        else
        {
            return returnValue;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////

}
