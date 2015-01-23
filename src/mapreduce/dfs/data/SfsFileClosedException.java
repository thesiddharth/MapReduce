package mapreduce.dfs.data;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 5:02 PM
 * To change this template use File | Settings | File Templates.
 */
//Exception thrown when a write is attempted on a closed file.
public class SfsFileClosedException extends SfsException {
    public SfsFileClosedException(String message, Throwable t) {
        super(message, t);
    }

    public SfsFileClosedException(Throwable t) {
        super(t);
    }
}
