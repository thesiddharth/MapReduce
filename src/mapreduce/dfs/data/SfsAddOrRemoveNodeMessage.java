package mapreduce.dfs.data;

import mapreduce.data.HostInfo;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/20/14
 * Time: 5:07 PM
 * To change this template use File | Settings | File Templates.
 */
//Message to add or remove a host, identified by ip and port number.
public class SfsAddOrRemoveNodeMessage extends SfsMessage
{
    private String ipAddress;
    private int portNo;

    public SfsAddOrRemoveNodeMessage(String ipAddress, int portNo) {
        this.ipAddress = ipAddress;
        this.portNo = portNo;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPortNo() {
        return portNo;
    }

    public void setPortNo(int portNo) {
        this.portNo = portNo;
    }
}
