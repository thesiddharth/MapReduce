/**
 * 
 */
package comm;

import java.util.Map;

/**
 * A progress report of a task in progress on one of the nodes.
 * @author surajd
 *
 */
public class MapTaskStatusMsg extends TaskStatusMsg {

	private static final long serialVersionUID = -8872658119132277615L;
	private Map<Integer , String>  mapOutputPaths;
	
	
	public MapTaskStatusMsg()
	{
		
	}


	/**
	 * @return the mapOutputPaths
	 */
	public Map<Integer,String> getMapOutputPaths() {
		return mapOutputPaths;
	}


	/**
	 * @param mapOutputPaths the mapOutputPaths to set
	 */
	public void setMapOutputPaths(Map<Integer, String> mapOutputPaths) {
		this.mapOutputPaths = mapOutputPaths;
	}
	
}
