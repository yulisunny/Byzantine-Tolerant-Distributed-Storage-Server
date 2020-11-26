package common.messages;

public interface KVMessage {
	
	enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		FAILED,
		SERVER_STOPPED,         /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
		SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */
		ADMIN_INIT_KVSERVER,			/* Admin - indicates the message is from admin */
		ADMIN_START,
		ADMIN_STOP,
		ADMIN_SHUTDOWN,
		ADMIN_LOCKWRITE,
		ADMIN_UNLOCKWRITE,
		ADMIN_MOVEDATA,
		ADMIN_COPYDATA,
		ADMIN_UPDATE,
		ADMIN_FILETRANSFER,
		ADMIN_FILETRANSFER_COMPLETE,
		ADMIN_SERVER_DEAD,
		ADMIN_SERVER_COMPROMISED,
		SERVER_HEART_BEAT,
		SERVER_HEART_BEAT_REPLY,
		ADMIN_DELETEDATA,
		ADMIN_PIN,
		ADMIN_REPLICATION,
		CLIENT_PUT_INTEGRITY_CHECK,
		CLIENT_SERVER_COMPROMISED,
		CLIENT_GET_INTEGRITY_CHECK,
		CLIENT_TRUE_GET,
		SUBSCRIBE,
		SUBSCRIBE_SUCCESS,
		NOTIFICATION,
		UNSUBSCRIBE,
		UNSUBSCRIBE_SUCCESS,
		SERVER_BROADCAST_UNSUBSCRIBE,
		ADMIN_UNSUBSCRIBE
	}

	/**
	 * @return a status string that is used to identify request types,
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return the value2 that is associated with this message,
	 * 		null if not value2 is associated.
	 */
	public String getMetadata();


	public String getSerializedMsg();

	public byte[] getBytes();

}



