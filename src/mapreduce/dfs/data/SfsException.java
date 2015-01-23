package mapreduce.dfs.data;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/8/14
 * Time: 12:51 AM
 * To change this template use File | Settings | File Templates.
 */
//Wraps the generic exception to create a customized one.
public class SfsException extends Exception {

    private static final long serialVersionUID = -128282937055597091L;

    public SfsException(String message, Throwable t)
    {
        super(message ,t);
    }

    public SfsException(Throwable t)
    {
        super(t);
    }

}
