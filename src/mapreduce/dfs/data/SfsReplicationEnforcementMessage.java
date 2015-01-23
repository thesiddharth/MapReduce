package mapreduce.dfs.data;

import mapreduce.dfs.SfsSegmentInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/17/14
 * Time: 11:59 PM
 * To change this template use File | Settings | File Templates.
 */
//Message to update state and enforce replication when a node is added or removed.
//Contains the ip to remove when a node is removed.
//Contains the information on updated segments.
//Nodes can iterate over this and see if they are supposed to pull in anything.
//Irrespective of this, they can update their local state.
public class SfsReplicationEnforcementMessage extends SfsUpdateMessage {

    private HashMap<SfsSegmentInfo, String> segmentInfoToUpdate;
    private String ipToRemove;

    public SfsReplicationEnforcementMessage() {
        this.segmentInfoToUpdate = new HashMap<>();
    }

    public HashMap<SfsSegmentInfo, String> getSegmentInfoToUpdate() {
        return segmentInfoToUpdate;
    }

    public void setSegmentInfoToUpdate(HashMap<SfsSegmentInfo, String> segmentInfoToUpdate) {
        this.segmentInfoToUpdate = segmentInfoToUpdate;
    }

    public String getIpToRemove() {
        return ipToRemove;
    }

    public void setIpToRemove(String ipToRemove) {
        this.ipToRemove = ipToRemove;
    }
}
