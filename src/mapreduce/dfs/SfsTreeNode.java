package mapreduce.dfs;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/18/14
 * Time: 8:14 PM
 * To change this template use File | Settings | File Templates.
 */
//Class representing an tree node. Has pointers to children and stores node type (directory or file),
// and file/dir information
public class SfsTreeNode implements Serializable
{
    private SfsTreeNodeType type;
    private SfsFileInfo nodeInfo;
    private ConcurrentHashMap<String, SfsTreeNode> children;

    SfsTreeNode()
    {
    }

    SfsTreeNode(SfsTreeNodeType type, SfsFileInfo nodeInfo) {
        this.type = type;
        this.nodeInfo = nodeInfo;
        this.children = new ConcurrentHashMap<>();
    }

    SfsTreeNode(SfsTreeNodeType type, SfsFileInfo nodeInfo, ConcurrentHashMap<String, SfsTreeNode> children) {
        this.type = type;
        this.nodeInfo = nodeInfo;
        this.children = children;
    }

    SfsTreeNodeType getType() {
        return type;
    }

    void setType(SfsTreeNodeType type) {
        this.type = type;
    }

    SfsFileInfo getNodeInfo() {
        return nodeInfo;
    }

    void setNodeInfo(SfsFileInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    ConcurrentHashMap<String, SfsTreeNode> getChildren() {
        return children;
    }

    void setChildren(ConcurrentHashMap<String, SfsTreeNode> children) {
        this.children = children;
    }

    void removeChild(String childName)
    {
        children.remove(childName);
    }

    SfsTreeNode getChild(String childName)
    {
        return children.get(childName);
    }

    void addChild(String fileOrDirName, SfsFileInfo fileInfo, SfsTreeNodeType type)
    {
        children.put(fileOrDirName, new SfsTreeNode(type, fileInfo));
    }

    void addDirectory(String dirName, SfsFileInfo dirInfo)
    {
        if(!children.containsKey(dirName))
            children.put(dirName, new SfsTreeNode(SfsTreeNodeType.DIRECTORY, dirInfo));
    }

    void addFile(String fileName, SfsFileInfo fileInfo)
    {
        if(!children.containsKey(fileName))
            children.put(fileName, new SfsTreeNode(SfsTreeNodeType.FILE, fileInfo));
    }


    void addDirectory(String dirName, SfsTreeNode dirNode) throws IOException
    {
        if(dirNode.getType()!=SfsTreeNodeType.DIRECTORY)
            throw new IOException("Not a directory");
        if(!children.containsKey(dirName))
            children.put(dirName, dirNode);
    }

    void addFile(String fileName, SfsTreeNode fileNode) throws IOException
    {
        if(fileNode.getType()!=SfsTreeNodeType.FILE)
            throw new IOException("Not a file");
        if(!children.containsKey(fileName))
            children.put(fileName, fileNode);
    }
}
