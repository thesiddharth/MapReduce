/**
 * 
 */
package mapreduce.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Util class to read configuration files at startup.
 * Configuration files contain key value pairs. ( e.g datanodes = 1,2,3 ; masterIp = "123.123..." etc);
 * @author surajd
 *
 */
public class ConfigManager {
	
	private static final String KEY_VALUE_SEPARATOR = "=";
	private static final String VALUE_SEPARATOR = ",";
	
	private static Map<String , Object> configKeyValues = null;
	
	
	public static void init(String configFilePath)
	{
		try 
		{
			configKeyValues = new ConcurrentHashMap<>();
			populateConfigValues(configFilePath);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Helper method to populate the config values read from a file.
	 * @param configFilePath
	 * @throws IOException
	 */
	private static void populateConfigValues(String configFilePath) throws IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(configFilePath));
		
		String input = null;
		
		while( (input = reader.readLine()) != null )
		{
			String[] splitInput = input.split(KEY_VALUE_SEPARATOR);
			
			String key = splitInput[0];
			
			String[] values = splitInput[1].split(VALUE_SEPARATOR);
			
			configKeyValues.put(key, Arrays.asList(values));
		}
		
		reader.close();
		
	}
	
	/**
	 * Reads the config value from the config values map.
	 * @param configKey
	 * @return
	 */
	public static Object getConfigValue(String configKey)
	{
		if(configKeyValues == null)
			throw new IllegalStateException("Reading value before initializing. Die.");
		
		if(!configKeyValues.containsKey(configKey))
			throw new IllegalArgumentException("Requested config key not present.");
		
		return configKeyValues.get(configKey);
	}

	

}
