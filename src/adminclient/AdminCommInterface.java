package adminclient;


import common.Metadata;

import java.io.IOException;

public interface AdminCommInterface {

    /**
     * Initialize the Server with the metadata, it's local cache size, and the cache replacement strategy,
     * and block it for client requests, i.e., all client requests are rejected with an SERVER_STOPPED error message;
     * Admin requests have to be processed.
     *
     * @param cacheSize size of the cache of the server
     * @param replacementStrategy string of "LRU, FIFO, LFU"
     * @param metadata mappings of server address to hash ranges
     * @throws Exception if initKVServer cannot be executed.
     */
    public void initKVServer(int cacheSize, String replacementStrategy, Metadata metadata) throws Exception;

    /**
     * Starts the Server, all client requests and all Admin requests are processed.
     */
    public void start() throws Exception;

    /**
     * Stops the Server, all client requests are rejected and only Admin requests are processed.
     */
    public void stop() throws Exception;

    /**
     * Exits the Server application.
     */
    public void shutDown() throws Exception;

    /**
     * Lock the Server for write operations.
     */
    public void lockWrite() throws Exception;

    /**
     * Unlock the KVserver for write operations.
     */
    public void unLockWrite() throws Exception;

    /**
     * Transfer a subset (range) of the Server's data to another
     * Server (reallocation before removing this server or adding a new Server to the ring);
     * send a notification to the Admin, if data transfer is completed.
     *
     * @param host server address name
     * @param port server port
     * @throws Exception failed to move data between servers
     */
    public void moveData(String host, String port, Metadata metadataRangeInfo) throws Exception;

    /**
     * Copy a subset (range) of the Server's data to another
     * Server (reallocation before removing this server or adding a new Server to the ring);
     * send a notification to the Admin, if data transfer is completed.
     *
     * @param host server address name
     * @param port server port
     * @throws Exception failed to move data between servers
     */
    public void copyData(String host, String port, Metadata metadataRangeInfo) throws Exception;

    /**
     * Update the metadata repository of this server.
     *
     * @param metadata mappings of server name to hash ranges
     * @see common.Metadata
     */
    public void update(Metadata metadata) throws Exception;

    public void connect() throws Exception;

    public void deleteData() throws Exception;

    public void pin() throws IOException;

    public void unsubscribe(String key, String value) throws IOException;
}
