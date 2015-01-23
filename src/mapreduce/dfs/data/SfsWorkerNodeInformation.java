package mapreduce.dfs.data;

import mapreduce.dfs.SfsSegmentInfo;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/18/14
 * Time: 4:38 PM
 * To change this template use File | Settings | File Templates.
 */
//Class encapsulating the state maintained at the Sfs Master (name node) for every worker(data node).
//Contains the total used space on a node, and a set of the references to the segments stored on it.
public class SfsWorkerNodeInformation {

    private long totalUsedStorage;
    private HashSet<SfsSegmentInfo> storedSegments;

    public SfsWorkerNodeInformation() {
        totalUsedStorage = 0;
        storedSegments = new HashSet<>();
    }

    public SfsWorkerNodeInformation(long totalUsedStorage) {
        this.totalUsedStorage = totalUsedStorage;
        storedSegments = new HashSet<>();
    }

    public long getTotalUsedStorage() {
        return totalUsedStorage;
    }

    public void setTotalUsedStorage(long totalUsedStorage) {
        this.totalUsedStorage = totalUsedStorage;
    }

    public HashSet<SfsSegmentInfo> getStoredSegments() {
        return storedSegments;
    }

    public void addStoredSegment(SfsSegmentInfo segmentInfo)
    {
        storedSegments.add(segmentInfo);
    }

    public void addStoredSegments(Collection<SfsSegmentInfo> segments)
    {
        storedSegments.addAll(segments);
    }

    public void addToBytesUsed(long bytesUsed)
    {
        totalUsedStorage+=bytesUsed;
    }
}
