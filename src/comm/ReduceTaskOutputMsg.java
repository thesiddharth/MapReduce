/**
 * 
 */
package comm;

import java.util.LinkedList;
import java.util.List;

import mapreduce.data.TaskOutput;

/**
 * Final output at the end of a reduce task
 * @author surajd
 *
 */
public class ReduceTaskOutputMsg extends Message
{

	private static final long serialVersionUID = -2061417555910921502L;
	
	List<TaskOutput> reduceTaskOutputs;
	
	
	public ReduceTaskOutputMsg()
	{
		reduceTaskOutputs = new LinkedList<>();
	}
	
	public List<TaskOutput> getReduceTaskOutputs()
	{
		return reduceTaskOutputs;
	}
	
	public void addTaskOutput(TaskOutput output)
	{
		reduceTaskOutputs.add(output);
	}

}
