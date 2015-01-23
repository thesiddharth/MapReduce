package mapreduce.dfs;

import mapreduce.dfs.data.SfsFileDoesNotExistException;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 4:46 PM
 * To change this template use File | Settings | File Templates.
 */
//Class representing the tree structure maintained at every data node as well as the master node.
//Logical view of the SFS file system
public class SfsTree
{

    private SfsTreeNode rootDirectory;

    protected SfsTree(SfsTreeNode rootDirectory) {
        this.rootDirectory = rootDirectory;
    }

    //Add a file to the tree
    protected void addFileToTree(SfsFileInfo fileInfo) throws IOException
    {
        String fileName = fileInfo.getFilePath().getTopLevelFileOrDirName();

        //Split on '/' to get the intervening directories.
        String[] sfsHierarchy = fileInfo.getFilePath().toString().split("/+");

        SfsTreeNode currNode = rootDirectory;

        for(int i = 1; i < sfsHierarchy.length - 1 ; i++)
        {
            currNode = currNode.getChild(sfsHierarchy[i]);
        }

        currNode.addFile(fileName, fileInfo);
    }

    //Add a single new directory to the tree
    protected void addDirectoryToTree(SfsFileInfo dirInfo) throws IOException
    {
        String fileName = dirInfo.getFilePath().getTopLevelFileOrDirName();

        String[] sfsHierarchy = dirInfo.getFilePath().toString().split("/+");

        SfsTreeNode currNode = rootDirectory;

        for(int i = 1; i < sfsHierarchy.length - 1 ; i++)
        {
            currNode = currNode.getChild(sfsHierarchy[i]);
        }

        currNode.addDirectory(fileName, dirInfo);

    }

    //Add a sequence of new directories to the tree
    protected void addDirectoriesToTree(SfsFileInfo dirInfo) throws IOException
    {
        String fileName = dirInfo.getFilePath().getTopLevelFileOrDirName();

        String[] sfsHierarchy = dirInfo.getFilePath().toString().split("/+");

        SfsTreeNode currNode = rootDirectory;
        SfsTreeNode prevNode = currNode;
        int i;
        StringBuilder currPath = new StringBuilder().append("sfs://");

        for(i = 1; i < sfsHierarchy.length - 1 ; i++)
        {
            prevNode = currNode;
            currNode = currNode.getChild(sfsHierarchy[i]);
            //Directories from this point onwards are not present
            if(currNode == null)
                break;
            else
                currPath.append(sfsHierarchy[i]).append("/");
        }

        //Add the subsequence of new directories
        if(i < sfsHierarchy.length - 1)
            currNode = prevNode;

        for(; i < sfsHierarchy.length -1; i++)
        {
            currPath.append(sfsHierarchy[i]);
            currNode.addDirectory(sfsHierarchy[i], new SfsFileInfo(Sfs.getPath(currPath.toString())));
            currNode = currNode.getChild(sfsHierarchy[i]);
            currPath.append("/");
        }

        currNode.addDirectory(fileName, dirInfo);

    }

    //Remove a directory or file node
    protected void removeDirectoryOrFile(SfsPath dirOrFilePath)
    {
        SfsTreeNode currNode = rootDirectory;

        String[] sfsHierarchy = dirOrFilePath.toString().split("/+");

        int i;
        for(i = 1; i < sfsHierarchy.length - 1 ; i++)
        {
            currNode = currNode.getChild(sfsHierarchy[i]);
        }

        currNode.removeChild(sfsHierarchy[i]);

    }

    //Update the file information stored at a node
    protected void updateFile(SfsFileInfo updatedFileInfo) throws SfsFileDoesNotExistException
    {
        try {
            SfsTreeNode currNode = rootDirectory;

            String[] sfsHierarchy = updatedFileInfo.getFilePath().toString().split("/+");

            int i;
            for(i = 1; i < sfsHierarchy.length - 1; i++)
            {
                currNode = currNode.getChild(sfsHierarchy[i]);
            }

            currNode.getChild(updatedFileInfo.getFilePath().getTopLevelFileOrDirName()).setNodeInfo(updatedFileInfo);
        }
        catch (NullPointerException e)
        {
            throw new SfsFileDoesNotExistException(new Exception(new StringBuilder().append(updatedFileInfo.getFilePath().toString()).append(" does not exist.").toString()));
        }

    }

    //Does this path exist - if it does, return the information object associated with it
    protected SfsFileInfo resolvePath(SfsPath path)
    {
        return resolvePath(path.toString());
    }

    //Does this path exist - if it does, return the information object associated with it, else return null.
    protected SfsFileInfo resolvePath(String path)
    {
        SfsTreeNode currNode = rootDirectory;

        try
        {
            String[] sfsHierarchy = path.split("/+");

            for(int i = 1; i < sfsHierarchy.length ; i++)
            {
                currNode = currNode.getChild(sfsHierarchy[i]);
            }

            return currNode.getNodeInfo();
        }
        catch (NullPointerException e)
        {
            return null;
        }
    }


    //Lookup the tree for this path. Throws an exception if not present
    protected SfsFileInfo getFileInfo(SfsPath path) throws SfsFileDoesNotExistException
    {
        return getFileInfo(path.toString());
    }

    //Lookup the tree for this path. Throws an exception if not present
    protected SfsFileInfo getFileInfo(String path) throws SfsFileDoesNotExistException
    {
        SfsTreeNode currNode = rootDirectory;

        try
        {
            String[] sfsHierarchy = path.split("/+");

            for(int i = 1; i < sfsHierarchy.length ; i++)
            {
                currNode = currNode.getChild(sfsHierarchy[i]);
            }

            return currNode.getNodeInfo();
        }
        catch(NullPointerException e)
        {
            throw new SfsFileDoesNotExistException(new Exception(new StringBuilder().append(path).append(" does not exist").toString()));
        }
    }

    //Get the root directory
    protected SfsTreeNode getRootNode()
    {
        return rootDirectory;
    }

    //Set the root directory
    protected void setRootNode(SfsTreeNode newRootNode)
    {
        this.rootDirectory = newRootNode;
    }
}


