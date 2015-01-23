package mapreduce.dfs.data;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 5:02 PM
 * To change this template use File | Settings | File Templates.
 */
// Exception thrown when a file that does not exist is accessed.
public class SfsFileDoesNotExistException extends SfsException {
    public SfsFileDoesNotExistException(String message, Throwable t) {
        super(message, t);
    }

    public SfsFileDoesNotExistException(Throwable t) {
        super(t);
    }
}
