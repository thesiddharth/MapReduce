package mapreduce.dfs;

import mapreduce.data.HostInfo;

import java.io.Serializable;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/6/14
 * Time: 1:27 AM
 * To change this template use File | Settings | File Templates.
 */
// Class representing the file information of an SFS file
public class SfsFileInfo implements Serializable{

    private static final long serialVersionUID = -4866784366583143607L;
    //Is this file closed
    private boolean isClosed;
    //Map of segment number to list of hosts on which segment resides
    private HashMap<Integer,LinkedHashSet<String>> segmentToHostListMap;
    //List of segment information encapsulated by this file
    private List<SfsSegmentInfo> segmentPaths;
    //The file path
    private SfsPath filePath;
    //Size of the file
    private long size;

    protected SfsFileInfo(boolean closed, SfsPath filePath) {
        isClosed = closed;
        this.filePath = filePath;
        segmentToHostListMap = new HashMap<>();
        segmentPaths = new ArrayList<>();
    }

    protected SfsFileInfo(SfsPath filePath) {
        isClosed = true;
        this.filePath = filePath;
        segmentToHostListMap = new HashMap<>();
        segmentPaths = new ArrayList<>();
    }

    /////////////////////////////////////////////////////////////////
    //Getters, setters and modifiers
    /////////////////////////////////////////////////////////////////

    public boolean isClosed() {
        return isClosed;
    }

    public void setClosed(boolean closed) {
        isClosed = closed;
    }

    public SfsPath getFilePath() {
        return filePath;
    }

    public void setFilePath(SfsPath filePath) {
        this.filePath = filePath;
    }

    public HashMap<Integer, LinkedHashSet<String>> getSegmentToHostListMap() {
        return segmentToHostListMap;
    }

    public void setSegmentToHostListMap(HashMap<Integer, LinkedHashSet<String>> segmentToHostListMap) {
        this.segmentToHostListMap = segmentToHostListMap;
    }

    public void addSegmentToHostListMapEntry(int segmentNumber, LinkedHashSet<String> hostInfo)
    {
        LinkedHashSet<String> hostList = segmentToHostListMap.get(segmentNumber);
        if(hostList==null)
            hostList = new LinkedHashSet<String>();
        hostList.addAll(hostInfo);
        segmentToHostListMap.put(segmentNumber, hostList);
    }

    public void addHostToSegmentHostList(int segmentNumber, String hostInfo)
    {
        segmentToHostListMap.get(segmentNumber).add(hostInfo);
    }

    public LinkedHashSet<String> getHostListForSegment(int segmentNumber)
    {
        return segmentToHostListMap.get(segmentNumber);
    }

    public List<SfsSegmentInfo> getSegmentPaths() {
        return segmentPaths;
    }

    public void setSegmentPaths(List<SfsSegmentInfo> segmentPaths) {
        this.segmentPaths = segmentPaths;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    /////////////////////////////////////////////////////////////////
}
