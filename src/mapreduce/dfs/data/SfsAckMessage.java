package mapreduce.dfs.data;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/20/14
 * Time: 5:18 PM
 * To change this template use File | Settings | File Templates.
 */
//Acknowledgement message
public class SfsAckMessage extends SfsMessage{

    private SfsException exception;

    public SfsAckMessage(SfsException exception) {
        this.exception = exception;
    }

    public SfsException getException() {
        return exception;
    }

    public void setException(SfsException exception) {
        this.exception = exception;
    }
}
