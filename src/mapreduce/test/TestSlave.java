package mapreduce.test;

import mapreduce.dfs.Sfs;
import mapreduce.slave.SlaveNode;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/16/14
 * Time: 5:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestSlave
{
    public static void main (String args[])
    {
        try
        {
            Sfs.initialize();
            System.out.println("SFS initialized on worker");
            SlaveNode slaveNode = new SlaveNode("dfsfd");
           
        }
        catch (Exception e)
        {
            e.printStackTrace();
            // fatal exception. die.
        }
    }
}
