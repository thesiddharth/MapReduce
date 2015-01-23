package mapreduce.dfs;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/5/14
 * Time: 8:24 PM
 * To change this template use File | Settings | File Templates.
 */
//Class representing an SFS path.
//Validates the input string path in constructor and wraps it along with the actual filesystem path, which is hidden
//outside the class
public class SfsPath implements Serializable{

    private static final long serialVersionUID = -4862861366583128507L;
    private transient Path actualPath;
    private String sfsPath;
    private transient Path homePath;

    protected SfsPath(String path, Path homePath) throws IllegalArgumentException
    {
        sfsPath = path;
        this.homePath = homePath;
        actualPath = validateAndFixPath(path, homePath, true);
    }

    //Concatenate two sfs paths
    public SfsPath resolve(String other) throws IOException
    {
        return new SfsPath(sfsPath.concat(other),homePath);
    }

    //Protected method for Sfs operations to get the actual filesystem path
    protected Path getActualPath()
    {
        return actualPath;
    }

    //Get an sfs path from the filesystem path and the homepath
    protected static SfsPath getSfsPath(Path normalPath, Path homePath) throws IllegalArgumentException
    {
        if(!normalPath.toString().contains(homePath.toString()))
            throw new IllegalArgumentException("Paths don't make sense");
        String sfsPath = "sfs://" + normalPath.toString().substring(homePath.toString().length()+1);
        sfsPath = sfsPath.replace('\\','/');
        return new SfsPath(sfsPath,homePath);
    }

    //Validates the SFS path - makes sure that the sfs path starts with 'sfs://' and doesn't contain ':' after that
    private Path validateAndFixPath(String path, Path homePath, boolean exceptionIfNotSFS) throws IllegalArgumentException
    {
        if( (path.startsWith("sfs://")) )
        {
            String subpath = path.substring(6);
            if(subpath.toString().contains(":"))
            {
                throw new IllegalArgumentException("Illegal SFS path");
            }
            return homePath.resolve(subpath);
        }
        else if(exceptionIfNotSFS)
        {
            throw new IllegalArgumentException("Not an SFS path");
        }
        else
        {
            return Paths.get(path);
        }
    }

    //GETTERS and SETTERS

    public String getTopLevelFileOrDirName()
    {
        return actualPath.getFileName().toString();
    }

    public void setActualPath(Path actualPath) {
        this.actualPath = actualPath;
    }

    public void setHomePath(Path homePath) {
        this.homePath = homePath;
    }

    @Override
    public String toString()
    {
        return sfsPath;
    }

}
