/**
 * 
 */
package mapreduce.data;

/**
 * Any exception that might have occured during the MR task processing.
 * @author surajd
 *
 */
public class MapReduceException extends Exception{

	private static final long serialVersionUID = -8789002160734646695L;
	
	public MapReduceException(String message)
	{
		super(message);
	}
	
	public MapReduceException(String message, Throwable t)
	{
		super(message , t);
	}
	
	public MapReduceException(Throwable t)
	{
		super(t);
	}

}
