package mapreduce.dfs;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: Sid
 * Date: 11/13/14
 * Time: 2:32 AM
 * To change this template use File | Settings | File Templates.
 */

//Class representing an SFS segment/chunk. (Note: the words 'segment' and 'chunk' are used interchangeably in this
//project
public class SfsSegmentInfo implements Serializable {

    private static final long serialVersionUID = -4866784366773143607L;
    private SfsPath sfsPath;
    //Size in bytes of this segment
    private long size;
    private long numRecords;
    //The formal file path represents the actual un-split file, which is maintained in the SFS tree and maintains
    //a list of these segments.
    private SfsPath formalFilePath;
    private int segmentNumber;

    public SfsSegmentInfo(SfsPath sfsPath, long size, SfsPath formalFilePath, long numRecords, int segmentNumber) {
        this.sfsPath = sfsPath;
        this.size = size;
        this.formalFilePath = formalFilePath;
        this.numRecords = numRecords;
        this.segmentNumber = segmentNumber;
    }

    //Dummy Constructor for bufferedReader
    public SfsSegmentInfo(SfsPath sfsPath, long size) {
        this.sfsPath = sfsPath;
        this.size = size;
    }

    /**
	 * @return the formalFilePath
	 */
	public SfsPath getFormalFilePath() {
		return formalFilePath;
	}

	/**
	 * @param formalFilePath the formalFilePath to set
	 */
	public void setFormalFilePath(SfsPath formalFilePath) {
		this.formalFilePath = formalFilePath;
	}

	public SfsPath getSfsPath() {
        return sfsPath;
    }

    public void setSfsPath(SfsPath sfsPath) {
        this.sfsPath = sfsPath;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(long numRecords) {
        this.numRecords = numRecords;
    }

    public int getSegmentNumber() {
        return segmentNumber;
    }

    public void setSegmentNumber(int segmentNumber) {
        this.segmentNumber = segmentNumber;
    }

    //OVERRIDEN EQUALS AND HASHCODE

    //Using only the SfsPath string and segment number gives us a unique and deterministic hashcode
    //for each segment info, allowing us to use it as an index in hashmaps.

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SfsSegmentInfo that = (SfsSegmentInfo) o;

        if (segmentNumber != that.segmentNumber) return false;
        if (!sfsPath.toString().equals(that.sfsPath.toString())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sfsPath.toString().hashCode();
        result = 31 * result + segmentNumber;
        return result;
    }
}

