package app_admin;

public interface AdminCommInterface {

    /**
     * Launches the storage server with the given cacheSize and replacementStrategy.
     *
     * @param numberOfNodes number of servers to bring up
     * @param cacheSize size of the cache on server
     * @param replacementStrategy LRU, LFU, FIFO cache replacement strategy
     */
    void initService(int numberOfNodes, int cacheSize, String replacementStrategy) throws Exception;


    /**
     * Starts all Server that participate in the service.
     */
    void start() throws Exception;

    /**
     * Stop all participating KVServers from processing client requests.
     */
    void stop() throws Exception;

    /**
     * Stop all KVServers and exists the remote processes.
     */
    void shutDown() throws Exception;

    /**
     * Create new Server with given cache size and replacement strategy.
     *
     * @param cacheSize size of cache on server
     * @param replacementStrategy LRU, LFU, FIFO cache replacement strategy
     */
    void addNode(int cacheSize, String replacementStrategy) throws Exception;

    /**
     * Remove a Server from the service at a given location.
     *
     * @param indexOfServer position of server to remove
     */
    void removeNode(int indexOfServer, boolean isAlreadyDead) throws Exception;

    Node getNode(String serverKey) throws IllegalArgumentException;

    int getNodeIndex(String serverKey) throws IllegalArgumentException;

    void unsubscribeBroadcast(String keyToUnsub, String clientToUnsub) throws Exception;
}
