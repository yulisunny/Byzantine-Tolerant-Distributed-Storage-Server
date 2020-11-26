package app_admin;

import adminclient.AdminCommInterface;
import common.HashRange;
import common.Metadata;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.SocketException;
import java.util.*;

/**
 * APIs responsible for communication between Admin and Servers.
 */
public class AdminStore implements app_admin.AdminCommInterface {

    private static Logger logger = Logger.getRootLogger();

    private static int SLEEP_TIME = 5000;

    private Metadata metadata; // initialized in initService
    private List<Node> allNodes;
    private List<Node> runningNodes;
    private List<Node> idleNodes;
    private String lastRemovedNodeKey;

    /**
     * Creates a new AdminStore.
     */
    public AdminStore() {
        allNodes = new ArrayList<>();
        runningNodes = new ArrayList<>();
        idleNodes = new ArrayList<>();
    }

    /**
     * Launches the storage server with the given cacheSize and replacementStrategy.
     *
     * @param numberOfNodes       number of servers to bring up
     * @param cacheSize           size of the cache on server
     * @param replacementStrategy LRU, LFU, FIFO cache replacement strategy
     */
    public void initService(int numberOfNodes, int cacheSize, String replacementStrategy) throws Exception {
        if (numberOfNodes <= 2) {
            logger.info("Admin tried to initialize with " + numberOfNodes + ". Must initialize at least 3.");
            throw new IllegalArgumentException("Must initialize no less than 3 nodes.");
        }

        // No Persistent Data file found, running init service for the first time
        if (lastRemovedNodeKey == null) {
            initServiceFirstTime(numberOfNodes, cacheSize, replacementStrategy);
            return;
        }

        // Load new servers if any
        parseServerConfigurations();

        // Use initNode to update metadata
        initExistingReplicas(numberOfNodes, cacheSize, replacementStrategy);
    }

    private void initServiceFirstTime(int numberOfNodes, int cacheSize, String replacementStrategy) throws IOException {
        parseServerConfigurations();

        List<Integer> range = new ArrayList<>();
        for (int j = 0; j < allNodes.size(); j++) {
            range.add(j);
        }

        // Randomly pick nodes to  start up
        List<String> runningNodeKeys = new ArrayList<>();
        int nodesToPick = Math.min(numberOfNodes, allNodes.size());
        for (int j = 0; j < nodesToPick; j++) {
            int index = new Random().nextInt(range.size());
            int indexIntoAllNodesList = range.remove(index);
            Node nodeToSsh = allNodes.get(indexIntoAllNodesList);
//            Node nodeToSsh;
//            if (j == 0) {
//                nodeToSsh = allNodes.get(0);
//                range.remove(0);
//            } else {
//                int index = new Random().nextInt(range.size());
//                int indexIntoAllNodesList = range.remove(index);
//                nodeToSsh = allNodes.get(indexIntoAllNodesList);
//            }
            runningNodeKeys.add(nodeToSsh.getKey());
            runningNodes.add(nodeToSsh);
            idleNodes.remove(nodeToSsh);
            runningNodeKeys.add(nodeToSsh.getKey());
            Runtime.getRuntime().exec("ssh -n " + nodeToSsh.getAddress() + " nohup java -jar " + System.getProperty("user.dir") + "/server.jar " + nodeToSsh.getAddress() + " " + nodeToSsh.getPort() + " &");
        }

        // Wait for Node to start up
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            logger.info("Encountered InterruptedException while waiting for nodes to start up.");
        }

        // Assign metadata
        metadata = new Metadata(runningNodeKeys);

        // Initialize servers with metadata
        for (int i = 0; i < runningNodes.size(); i++) {
            Node currNode = runningNodes.get(i);
            adminclient.AdminCommInterface store = currNode.getStore();
            try {
                store.initKVServer(cacheSize, replacementStrategy, metadata);
                store.deleteData();
            } catch (Exception e) {
                logger.error(String.format("Failed to initialize server with address %s.", currNode.getKey()), e);
            }
        }
        logger.debug("Metadata: " + metadata.getSerializedForm());
    }

    private void parseServerConfigurations() throws IOException {
        logger.info("Parsing the configuration file.");

        File inputFile = new File("admin.config");
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for admin.config file");
            throw e;
        }

        String currentLine;
        while ((currentLine = reader.readLine()) != null) {
            String trimmedLine = currentLine.trim();
            String[] tokens = trimmedLine.split("\\s+");

            String name = tokens[0];
            String address = tokens[1];
            String port = tokens[2];
            Node node = new Node(name, address, port);
            allNodes.add(node);
            idleNodes.add(node);
        }
    }
    private void copyDataFromServerToServer(Node fromServerNode, Node toServerNode, HashRange neededHashRange) {
        try {
            // Send metadata update to successor
            // Move files from to be removed store to successor
            AdminCommInterface copyingFromStore = fromServerNode.getStore();
            copyingFromStore.lockWrite();
            copyingFromStore.copyData(toServerNode.getAddress(), toServerNode.getPort(), new Metadata(toServerNode.getKey(), neededHashRange));
            copyingFromStore.unLockWrite();
        } catch (Exception e) {
            logger.error(String.format("Failed to move HashRange %s from %s to %s.",
                    neededHashRange.toString(), fromServerNode.getKey(), toServerNode.getKey(), e));
        }
    }
    private void initExistingReplicas(int numberOfNodes, int cacheSize, String replacementStrategy) throws Exception {
        // Start up the server through ssh, handles check for lastRemovedNodeKey
        Node lastRemovedNodeWithData = getNodeToStartUp();
        Node replica1 = getNodeToStartUp();
        Node replica2 = getNodeToStartUp();
        logger.debug(String.format("Initializing servers: %s, %s and %s",
                lastRemovedNodeWithData.getKey(), replica1.getKey(), replica2.getKey()));

        // Create start metadata
        metadata = new Metadata(Arrays.asList(lastRemovedNodeWithData.getKey(), replica1.getKey(), replica2.getKey()));
        HashRange copiedHashRange = new HashRange(lastRemovedNodeWithData.getMD5Key(), lastRemovedNodeWithData.getMD5Key());

        // Initialize starter servers
        AdminCommInterface store = lastRemovedNodeWithData.getStore();
        sshStartUpServer(lastRemovedNodeWithData);
        store.connect();
        store.initKVServer(cacheSize, replacementStrategy, new Metadata(Arrays.asList(lastRemovedNodeWithData.getKey())));

        adminclient.AdminCommInterface storeR1 = replica1.getStore();
        sshStartUpServer(replica1);
        storeR1.connect();
        storeR1.initKVServer(cacheSize, replacementStrategy, new Metadata(Arrays.asList(lastRemovedNodeWithData.getKey(), replica1.getKey())));
        storeR1.deleteData();
        copyDataFromServerToServer(lastRemovedNodeWithData, replica1, copiedHashRange);

        AdminCommInterface storeR2 = replica2.getStore();
        sshStartUpServer(replica2);
        storeR2.connect();
        storeR2.initKVServer(cacheSize, replacementStrategy, metadata);
        storeR2.deleteData();
        copyDataFromServerToServer(lastRemovedNodeWithData, replica2, copiedHashRange);

        // Make sure the three nodes have the same metadata
        broadcastMetadataUpdate();

        // Reset counter
        numberOfNodes -= 3;

        // Add in remaining ones
        while (numberOfNodes > 0 && idleNodes.size() > 0) {
           addNode(cacheSize, replacementStrategy);
           numberOfNodes -= 1;
        }
    }


    private AdminCommInterface initNode(int cacheSize, String replacementStrategy) {
        // Start up the server through ssh, handles check for lastRemovedNodeKey
        Node newNode = getNodeToStartUp();
        adminclient.AdminCommInterface store = newNode.getStore();
        sshStartUpServer(newNode);

        try {
            // Set up communication channel
            store.connect();

            // Retrieve successor before updating metadata
            String successorKey = metadata.getSuccessorServer(newNode.getMD5Key());

            // Update metadata hash
            metadata.addNewServer(newNode.getKey(), newNode.getMD5Key());
            store.initKVServer(cacheSize, replacementStrategy, metadata);

            // If node is the only one in service, no need to move data from successor
            if (successorKey == null) {
                return store;
            }

            // Transport data from successor node to new node
            adminclient.AdminCommInterface successorStore = getNode(successorKey).getStore();
            successorStore.lockWrite();
            Metadata meta = new Metadata();
            meta.setHashRange(newNode.getAddress(),
                    Integer.parseInt(newNode.getPort()),
                    metadata.getHashRange(newNode.getKey()));
            logger.debug(String.format("Moving data from %s to %s", successorKey, newNode.getKey()));
            successorStore.moveData(newNode.getAddress(), newNode.getPort(), meta);
            successorStore.unLockWrite();
        } catch (Exception e) {
            logger.info("Error initializing server.", e);
            metadata.removeServer(newNode.getKey());
        }
        return store;
    }

    /**
     * Starts all Server that participate in the service.
     */
    @Override
    public void start() throws Exception {
        checkInitializations();

        for (Node node : runningNodes) {
            adminclient.AdminCommInterface store = node.getStore();
            if (store != null) {
                try {
                    store.start();
                } catch (Exception e) {
                    logger.error("Failed to start up all servers.", e);
                }
            }
        }
    }

    /**
     * Stop all participating KVServers from processing client requests.
     */
    @Override
    public void stop() throws Exception {
        checkInitializations();

        for (Node node : runningNodes) {
            AdminCommInterface store = node.getStore();
            if (store != null) {
                try {
                    store.stop();
                } catch (Exception e) {
                    logger.error("Failed to stop all servers.", e);
                }
            }
        }
    }

    @Override
    public void unsubscribeBroadcast(String keyToUnsub, String clientToUnsub) throws Exception {
        for (Node node : runningNodes) {
            adminclient.AdminCommInterface store = node.getStore();
            if (store != null) {
                try {
                    store.unsubscribe(keyToUnsub, clientToUnsub);
                } catch (Exception e) {
                    logger.error("Failed to stop all servers.", e);
                }
            }
        }
    }

    /**
     * Stop all KVServers and exists the remote processes.
     */
    @Override
    public void shutDown() throws Exception {
        // No servers to shutdown
        if (allNodes.size() == 0 || runningNodes.size() == 0) {
            return;
        }

        // Extract the keys first to prevent Concurrent Modifications of Objects
        List<String> serverKeys = new ArrayList<>();
        for (int i = 0; i < runningNodes.size(); i++) {
            serverKeys.add(runningNodes.get(i).getKey());
        }

        // Remove one server at a time until 3 servers left
        for (String serverKey : serverKeys) {
            try {
                removeNode(getNodeIndex(serverKey), false);
            } catch(IllegalStateException e) {
                logger.debug("Only have 3 servers left to shutdown.");
                break;
            } catch (Exception e) {
                logger.error("Failed to shutdown server " + serverKey, e);
            }
        }

        lastRemovedNodeKey = runningNodes.get(0).getKey();
        logger.debug("Saving service configuration before shutdown. Last known server: " + lastRemovedNodeKey);

        // Shut down remaining 3
        for (int i = 0; i < runningNodes.size(); i++) {
            try {
                runningNodes.get(i).getStore().shutDown();
            } catch (Exception e) {
                logger.debug("Failed to shutdown dead server " + runningNodes.get(i).getKey());
            }
        }

        runningNodes.clear();
        allNodes.clear();
        idleNodes.clear();
    }

    /**
     * Create new Server with given cache size and replacement strategy.
     *
     * @param cacheSize           size of cache on server
     * @param replacementStrategy LRU, LFU, FIFO cache replacement strategy
     */
    public void addNode(int cacheSize, String replacementStrategy) throws Exception {
        checkInitializations();

        if (idleNodes.size() == 0) {
            logger.error("No more nodes available. Reach max capacity.");
            return;
        }

        try {
            // Start up the server through ssh, handles check for lastRemovedNodeKey
            Node newNode = getNodeToStartUp();
            AdminCommInterface store = newNode.getStore();
            sshStartUpServer(newNode);

            // Set up communication channel
            store.connect();

            // Update metadata hash
            Map<String, HashRange> serversNeededToCopyFrom = metadata.addNewServer(newNode.getKey(), newNode.getMD5Key());
            store.initKVServer(cacheSize, replacementStrategy, metadata);
            store.deleteData();

            // Move the pieces of data to new server
            for (Map.Entry<String, HashRange> entry:serversNeededToCopyFrom.entrySet()) {
                String movingFromServerKey = entry.getKey();
                HashRange movingHashRange = entry.getValue();

                try {
                    AdminCommInterface movingFromStore = getNode(movingFromServerKey).getStore();
                    movingFromStore.lockWrite();
                    logger.debug(String.format("Moving data from %s to %s", movingFromServerKey, newNode.getKey()));
                    movingFromStore.moveData(newNode.getAddress(), newNode.getPort(), new Metadata(newNode.getKey(), movingHashRange));
                    movingFromStore.unLockWrite();
                } catch (SocketException e) {
                    // Copying the same hash range from somewhere else
                    logger.debug("Cannot move data from dead server. Attempting to copy data from other replicas.");
                    copyingHashRangeToServer(newNode, movingHashRange);
                }
            }

            // Start to receive requests
            store.start();
            broadcastMetadataUpdate();
        } catch (Exception e) {
            logger.error("Encountered error when adding node", e);
        }

        logger.debug("Metadata: " + metadata.getSerializedForm());
    }

    private void copyingHashRangeToServer(Node receivingNode, HashRange neededHashRange) {
        List<String> readableServers = metadata.getReadableServers(neededHashRange.getEndHash());
        Boolean movedSucceed = false;

        // Attempt to move data from these servers until one succeed
        for (int i = 0; i < readableServers.size() && !movedSucceed; i++) {
            String copyingFromServer = readableServers.get(i);
            Node copyingFromServerNode = getNode(copyingFromServer);
            AdminCommInterface copyingFromStore = copyingFromServerNode.getStore();

            try {
                // Send metadata update to successor, move files from to be removed store to successor
                copyingFromStore.lockWrite();
                copyingFromStore.update(metadata);
                copyingFromStore.copyData(receivingNode.getAddress(), receivingNode.getPort(),
                        new Metadata(receivingNode.getKey(), neededHashRange));
                copyingFromStore.unLockWrite();
                movedSucceed = true;
            } catch (Exception e) {
                try {
                    copyingFromStore.unLockWrite();
                } catch (Exception e1) {}
                logger.error(String.format("Failed to move HashRange %s from %s to %s. Trying a different one.",
                        neededHashRange.toString(), copyingFromServer, receivingNode.getKey(), e));
            }

        }
    }


    private Node getNodeToStartUp() {
        // No more nodes running, need to add back the last removed node first if exists
        if (runningNodes.size() == 0 && lastRemovedNodeKey != null) {
            for (int i = 0; i < idleNodes.size(); i++) {
                if(idleNodes.get(i).getKey().equals(lastRemovedNodeKey)) {
                    Node ret = idleNodes.remove(i);
                    runningNodes.add(ret);
                    lastRemovedNodeKey = null;
                    return ret;
                }
            }
        }

        // Randomly select a node
        int randIdx = new Random().nextInt(idleNodes.size());
        Node newNode = idleNodes.remove(randIdx);
        runningNodes.add(newNode);
        return newNode;
    }

    private void sshStartUpServer(Node newNode) {
        logger.info("Attempting to bring up server " + newNode.getKey());
        try {
            Runtime.getRuntime().exec("ssh -n " + newNode.getAddress() + " nohup java -jar " +
                    System.getProperty("user.dir") + "/server.jar " + newNode.getAddress() + " " + newNode.getPort() + " &");
            Thread.sleep(SLEEP_TIME);
        } catch (IOException e) {
            logger.error("Received I/O error when starting up server.");
        } catch (InterruptedException e) {
            logger.info("Encountered InterruptedException while waiting for nodes to start up.");
        }
    }

    private void broadcastMetadataUpdate() {
        for (Node node : runningNodes) {
            try {
                AdminCommInterface store = node.getStore();
                store.update(metadata);
            } catch (Exception e) {
                logger.debug("Potentially dead running node " + node.getKey());
            }
        }
    }

    /**
     * Remove a Server from the service at a random location.
     *
     * @param indexOfServer position of server in the configuration file, where the first server
     *                      has an index of 0
     */
    @Override
    public void removeNode(int indexOfServer, boolean isAlreadyDead) throws Exception {
        checkInitializations();

        if (indexOfServer >= allNodes.size()) {
            logger.error("Received invalid indexOfServer to remove.");
            return;
        }

        if (runningNodes.size() <= 3) {
            logger.info("Attempted to remove node when there are 3 or less servers running.");
            throw new IllegalStateException("Number of nodes cannot drop below 3.");
        }

        // Attempt to remove
        Node nodeToRemove = allNodes.get(indexOfServer);
        Boolean removed = runningNodes.remove(nodeToRemove);
        if (!removed) {
            logger.info("Currently indexed server is idle, no need to remove.");
            return;
        }

        // Add back to idle list
        idleNodes.add(nodeToRemove);

        // Lock the server that is getting removed to prevent further writes
        AdminCommInterface nodeToRemoveStore = nodeToRemove.getStore();
        if (!isAlreadyDead) {
            nodeToRemoveStore.lockWrite();
        }

        // Reconcile servers to new topology
        Map<String, HashRange> serversNeedUpdate = metadata.removeServer(nodeToRemove.getKey());
        for(Map.Entry<String, HashRange> entry:serversNeedUpdate.entrySet()) {
            String receivingServerKey = entry.getKey();
            HashRange neededHashRange = entry.getValue();
            Node receivingNode;

            // Receiving node might be dead already, check first
            try {
                receivingNode = getNode(receivingServerKey);
            } catch (IllegalArgumentException e) {
                logger.info("Attempting to move data to failed receiving node " + receivingServerKey + " . Skipping.");
                continue;
            }

            List<String> readableServers = metadata.getReadableServers(neededHashRange.getEndHash());
            Boolean movedSucceed = false;

            // Attempt to move data from these servers until one succeed
            for (int i = 0; i < readableServers.size() && !movedSucceed; i++) {
                String copyingFromServer = readableServers.get(i);
                Node copyingFromServerNode = getNode(copyingFromServer);
                AdminCommInterface copyingFromStore = copyingFromServerNode.getStore();

                try {
                    // Send metadata update to successor
                    // Move files from to be removed store to successor
                    copyingFromStore.lockWrite();
                    copyingFromStore.update(metadata);
                    copyingFromStore.copyData(receivingNode.getAddress(), receivingNode.getPort(), new Metadata(receivingServerKey, neededHashRange));
                    copyingFromStore.unLockWrite();
                    movedSucceed = true;
                } catch (Exception e) {
                    try {
                        copyingFromStore.unLockWrite();
                    } catch (Exception e1) {}
                    logger.error(String.format("Failed to move HashRange %s from %s to %s.",
                            neededHashRange.toString(), copyingFromServer, receivingServerKey, e));
                }

            }
        }

        // Clean up
        if (!isAlreadyDead){
            nodeToRemoveStore.stop();
            nodeToRemoveStore.unLockWrite();
            nodeToRemoveStore.shutDown();
        }

        // Remember last removed node if configuration <= 3
        if (runningNodes.size() <= 3) {
            lastRemovedNodeKey = runningNodes.get(0).getKey();
        }

        broadcastMetadataUpdate();
        logger.debug("metadata: " + metadata.getSerializedForm());
    }

    public Node getNode(String serverKey) throws IllegalArgumentException {
        for (Node node : runningNodes) {
            if (node.getKey().equals(serverKey)) {
                return node;
            }
        }
        throw new IllegalArgumentException("Cannot find a running server with key " + serverKey);
    }

    public int getNodeIndex(String serverKey) throws IllegalArgumentException {

        for (int index = 0; index < allNodes.size(); index++) {
            if (allNodes.get(index).getKey().equals(serverKey)) {
                return index;
            }
        }
        throw new IllegalArgumentException("Cannot find a server with key " + serverKey);
    }

    public void checkInitializations() throws IllegalStateException {
        if (allNodes == null || this.allNodes.size() == 0) {
            logger.error("Service is not initialized. Please run initService first.");
            throw new IllegalStateException("Service is not initialized. Please run initService first.");
        }
    }

    public List<String> getRunningServers() {
        List<String> ret = new ArrayList<>();
        for (Node node:runningNodes) {
            ret.add(node.getKey());
        }
        return ret;
    }
}
