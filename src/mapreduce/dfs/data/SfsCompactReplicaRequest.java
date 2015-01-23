package mapreduce.dfs.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/18/14
 * Time: 12:37 AM
 * To change this template use File | Settings | File Templates.
 */
//Replica request object.
//Has a list of paths needed.
public class SfsCompactReplicaRequest implements Serializable{

    private static final long serialVersionUID = -4862861366583128917L;
    private List<String> paths;

    public SfsCompactReplicaRequest() {
        paths = new ArrayList<>();
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        this.paths.add(path);
    }

    public void addPaths(Collection<String> paths) {
        this.paths.addAll(paths);
    }
}

