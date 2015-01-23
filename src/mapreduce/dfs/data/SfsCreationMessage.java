package mapreduce.dfs.data;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/17/14
 * Time: 11:59 PM
 * To change this template use File | Settings | File Templates.
 */
//File creation message.
//Can indicate if replication is required and if so, which segments of the file need to be pulled.
public class SfsCreationMessage extends SfsUpdateMessage {

    private boolean replicate = false;
    private Set<Integer> segmentsToReplicate;

    public boolean replicate() {
        return replicate;
    }

    public void setReplicate(boolean replicateHere) {
        this.replicate = replicateHere;
    }

    public Set<Integer> getSegmentsToReplicate() {
        return segmentsToReplicate;
    }

    public void setSegmentsToReplicate(Set<Integer> segmentsToReplicate) {
        this.segmentsToReplicate = segmentsToReplicate;
    }
}
