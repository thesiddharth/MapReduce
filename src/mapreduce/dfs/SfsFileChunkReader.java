/**
 * 
 */
package mapreduce.dfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

/**
 * @author surajd
 *
 */
//File splitting logic is encapsulated into this reader class
public class SfsFileChunkReader {
	
	private long currentReadSize;
    //Chunk size in bytes, calculated from config file setting read in Sfs.
    private static final double CHUNK_SIZE_IN_BYTES = Sfs.getChunkSize() * 1024 * 1024;
	
	private BufferedReader fileReader;
	
	
	public SfsFileChunkReader(Path filePath) throws IOException
	{
		initialize(filePath);
	}

	private void initialize(Path filePath) throws IOException {
            currentReadSize = 0;
            fileReader = new BufferedReader(new FileReader(filePath.toString()));
	}

    public boolean hasNextChunk()
    {
        try
        {
            return fileReader.ready();
        }
        catch (IOException e)
        {
            return false;
        }
    }

    // Can more info be put into this segment chunk, and is there more to be read from the underlying file?
	public boolean hasNextEntry() {
		if(currentReadSize < CHUNK_SIZE_IN_BYTES && hasNextChunk())
            return true;
        else
        {
            currentReadSize =0;
            return false;
        }
	}

    //Maintain the current chunk size while reading
    public String getNextEntry() throws IOException  {
		String returnValue = fileReader.readLine();
        currentReadSize += returnValue.getBytes().length;
        return returnValue;
	}
	
	public void close() throws IOException
	{
		fileReader.close();
	}

}
