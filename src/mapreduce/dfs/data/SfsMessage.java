package mapreduce.dfs.data;

import mapreduce.data.HostInfo;

import java.io.Serializable;

//Generic sfs message class
public class SfsMessage implements Serializable {

	private static final long serialVersionUID = -51371207180888027L;
	
	public static enum SfsMessageType
	{
		CREATE_FILE ,
		CREATE_DIRECTORY,
		REMOVE_FILE,
		REMOVE_DIRECTORY,
        ACK_MESSAGE,
        UPDATE_INFO,
        RECONSTRUCT_TREE,
        ENFORCE_REPLICAS,
        ADD_HOST,
        REMOVE_HOST;
	}

	// type of the message.
	private SfsMessageType type;
    //sender details
    private HostInfo senderHostInfo;

	/**
	 * @return the type
	 */
	public SfsMessageType getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(SfsMessageType type) {
		this.type = type;
	}

    public HostInfo getSenderHostInfo() {
        return senderHostInfo;
    }

    public void setSenderHostInfo(HostInfo senderHostInfo) {
        this.senderHostInfo = senderHostInfo;
    }
}
