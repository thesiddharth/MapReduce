package mapreduce.dfs.data;

import mapreduce.dfs.SfsFileInfo;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 12:56 AM
 * To change this template use File | Settings | File Templates.
 */
//General purpose update message.
//Contains information on the updated file.
public class SfsUpdateMessage extends SfsMessage {

    private SfsFileInfo updatedFileInfo;

    public SfsFileInfo getUpdatedFileInfo() {
        return updatedFileInfo;
    }

    public void setUpdatedFileInfo(SfsFileInfo updatedFileInfo) {
        this.updatedFileInfo = updatedFileInfo;
    }
}
