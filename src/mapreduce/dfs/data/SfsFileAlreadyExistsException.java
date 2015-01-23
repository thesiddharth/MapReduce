package mapreduce.dfs.data;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 5:02 PM
 * To change this template use File | Settings | File Templates.
 */
//Sfs file already exists.
public class SfsFileAlreadyExistsException extends SfsException {
    public SfsFileAlreadyExistsException(String message, Throwable t) {
        super(message, t);
    }

    public SfsFileAlreadyExistsException(Throwable t) {
        super(t);
    }
}
