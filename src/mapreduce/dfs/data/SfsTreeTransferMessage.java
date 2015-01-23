package mapreduce.dfs.data;

import mapreduce.dfs.SfsTreeNode;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/18/14
 * Time: 7:55 PM
 * To change this template use File | Settings | File Templates.
 */
//Message to transfer the image of the global state tree to a node.
//Used to initialize a new node when it comes up.
public class SfsTreeTransferMessage extends SfsMessage
{
    private SfsTreeNode rootNode;

    public SfsTreeTransferMessage(SfsTreeNode rootNode) {
        this.rootNode = rootNode;
    }

    public SfsTreeNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(SfsTreeNode rootNode) {
        this.rootNode = rootNode;
    }
}
