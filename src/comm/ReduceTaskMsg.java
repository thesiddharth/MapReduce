package comm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Represents the information in a message to start a reduce task.
 * @author surajd
 *
 */
public class ReduceTaskMsg extends Message{

	private static final long serialVersionUID = -5629309006336055534L;
	
	private Map<Integer , List<String> > reducerInputPaths;
	
	public ReduceTaskMsg()
	{
		this.reducerInputPaths = new HashMap<Integer, List<String>>();
	}
	
	
	/**
	 * @return the reducerInputPaths
	 */
	public Map<Integer, List<String>> getReducerInputPaths() {
		return reducerInputPaths;
	}
	/**
	 * @param reducerInputPaths the reducerInputPaths to set
	 */
	public void setReducerInputPaths(Map<Integer, List<String>> reducerInputPaths) {
		this.reducerInputPaths = reducerInputPaths;
	}
	
	
	
	
}
