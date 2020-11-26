package common;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Class to keep track of mappings between server addresses and the range of hashes
 * the server is providing support for.
 *
 * @see common.HashRange
 */
public class Metadata {

    private final Map<String, HashRange> mappings;
    private final TreeMap<String, String> hashToAddr;

    /**
     * Create metadata with no initializations.
     */
    public Metadata() {
        mappings = new HashMap<>();
        hashToAddr = new TreeMap<>();
    }

    /**
     * Create metadata with no initializations.
     */
    public Metadata(String serverKey, HashRange moveDataHashRange) {
        mappings = new HashMap<>();
        hashToAddr = new TreeMap<>();
        mappings.put(serverKey, moveDataHashRange);
    }

    /**
     * Create metadata from existing mappings.
     *
     * @param inputMappings mapping of server address to respective HashRange
     */
    private Metadata(Map<String, HashRange> inputMappings) {
        mappings = inputMappings;
        hashToAddr = new TreeMap<>();
        for (Map.Entry<String, HashRange> entry : mappings.entrySet()) {
            String serverKey = entry.getKey();
            String serverHash = entry.getValue().getEndHash();
            hashToAddr.put(serverHash, serverKey);
        }
    }

    /**
     * Create metadata from a list of server keys.
     *
     * @param serverKeys list of server keys of form address:port
     */
    public Metadata(List<String> serverKeys) {
        mappings = new HashMap<>();
        hashToAddr = new TreeMap<>();

        // Add in each server hash into a tree
        for (String serverKey : serverKeys) {
            String serverHash = HashRange.getMd5Hash(serverKey);
            hashToAddr.put(serverHash, serverKey);
        }

        // Add in reverse lookup mechanism
        String prevHash = hashToAddr.lastKey();
        String prevPrevHash = getImmediatePredecessor(prevHash).getKey();
        for (Map.Entry<String, String> entry : hashToAddr.entrySet()) {
            String serverHash = entry.getKey();
            String serverKey = entry.getValue();
            String readStart = getImmediatePredecessor(prevPrevHash).getKey();
            mappings.put(serverKey, new HashRange(prevHash, serverHash, readStart));
            prevPrevHash = prevHash;
            prevHash = serverHash;
        }
    }

    public Integer size() {
        return hashToAddr.size();
    }

    /**
     * Obtain the server key for the immediate successor of the input serverHash.
     * @param newServerHash md5 hash of an server key
     * @return server key(address:port) that is successor of input server hash
     */
    public String getSuccessorServer(String newServerHash) {
        Map.Entry<String, String> successor = hashToAddr.ceilingEntry(newServerHash);
        if (successor == null) {
            successor = hashToAddr.firstEntry();
        }
        return successor != null ? successor.getValue() : null;
    }


    /**
     * Obtain the server key for the immediate predecessor of the input serverHash.
     * @param newServerHash md5 hash of an server key
     * @return server key(address:port) that is predecessor of input server hash
     */
    public String getPredecessorServer(String newServerHash) {
        Map.Entry<String, String> pre = hashToAddr.floorEntry(newServerHash);
        if (pre == null) {
            pre = hashToAddr.lastEntry();
        }
        return pre != null ? pre.getValue() : null;
    }

    /**
     * For client usage: randomly pick one server to read from
     *
     * @param dataKey
     * @return
     */
    public String getReadableServer(String dataKey) {
        List<String> servers = getReadableServers(dataKey);
        Integer randn = new Random().nextInt(servers.size());
        return servers.get(randn);
    }

    /**
     * Returns all the server keys that contains the current dataHash
     * @param dataHash
     * @return
     */
    public List<String> getReadableServers(String dataHash) {
        Map.Entry<String, String> successor = hashToAddr.ceilingEntry(dataHash);
        if (successor == null) {
            successor = hashToAddr.firstEntry();
        }

        // Brute force lookup for now
        Map.Entry<String, String> replica1Entry = getImmediateSuccessor(successor.getKey());
        String replica1ServerKey = replica1Entry.getValue();
        String replica2ServerKey = getImmediateSuccessor(replica1Entry.getKey()).getValue();
        return Arrays.asList(successor.getValue(), replica1ServerKey, replica2ServerKey);
    }

    /**
     * Returns the replica servers for the current data hash
     * @param dataHash
     * @return
     */
    public List<String> getBackups(String dataHash) {
        Map.Entry<String, String> successor = hashToAddr.ceilingEntry(dataHash);
        if (successor == null) {
            successor = hashToAddr.firstEntry();
        }

        // Brute force lookup for now
        Map.Entry<String, String> replica1Entry = getImmediateSuccessor(successor.getKey());
        String replica1ServerKey = replica1Entry.getValue();
        String replica2ServerKey = getImmediateSuccessor(replica1Entry.getKey()).getValue();
        return Arrays.asList(replica1ServerKey, replica2ServerKey);
    }

    public Set<String> getServers() {
        return mappings.keySet();
    }

    public Map.Entry<String, String> getImmediateSuccessor(String dataHash) {
        Map.Entry<String, String> successor = hashToAddr.higherEntry(dataHash);
        if (successor == null) {
            successor = hashToAddr.firstEntry();
        }
        return successor;
    }

    private Map.Entry<String, String> getImmediatePredecessor(String dataHash) {
        Map.Entry<String, String> prede = hashToAddr.lowerEntry(dataHash);
        if (prede == null) {
            prede = hashToAddr.lastEntry();
        }
        return prede;
    }

    public String getWritableServer(String dataHash) {
        return getSuccessorServer(dataHash);
    }

    /**
     * Update metadata to reflect the addition of new server.
     *
     * @param newServerKey key of the new server to be added
     * @param newServerHash hex representation of MD5 hash of the server key
     */
    public Map<String, HashRange> addNewServer(String newServerKey, String newServerHash) {
        String predecessorServerKey = getPredecessorServer(newServerHash);
        String successorServerKey = getSuccessorServer(newServerHash);

        // No server was initialized, will no longer enter after M2
        if (successorServerKey == null) {
            hashToAddr.put(newServerHash, newServerKey);
            mappings.put(newServerKey, new HashRange(newServerHash, newServerHash, newServerHash));
            return new HashMap<>();
        }

        // Update HashRanges
        hashToAddr.put(newServerHash, newServerKey);
        mappings.put(newServerKey, new HashRange(
                mappings.get(predecessorServerKey).getEndHash(),
                newServerHash,
                mappings.get(successorServerKey).getReadStartHash()));
        mappings.get(successorServerKey).setStartHash(newServerHash);

        // Update Read Start Hash
        return updateReadStartWithAddedNode(newServerHash);

//        // Update Replicas
//        Map.Entry<String, Servers> predecessor = getImmediatePredecessor(newServerKey);
//        Map.Entry<String, Servers> prePreredecessor = getImmediatePredecessor(predecessor.getKey());
//        predecessor.getValue().setReplica1(newServerKey);
//        prePreredecessor.getValue().setReplica2(newServerKey);
//        newServerEntry.setReplica1(successorServerKey);
//        newServerEntry.setReplica2(hashToAddr.higherKey(successorServerKey));
    }

    /**
     * Update metadata to reflect the removal of a server with the given serverKey.
     *
     * @param serverKey server key of the form address:port
     * @return a map of servers and the data they need from other servers
     */
    public Map<String, HashRange> removeServer(String serverKey) {
        if (!mappings.containsKey(serverKey)) {
            return Collections.emptyMap();
        }

        // Remove server hash so that will not get self when getting successor
        String serverHash = mappings.get(serverKey).getEndHash();
        hashToAddr.remove(serverHash);
        mappings.remove(serverKey);
        String successorServerKey = getSuccessorServer(serverHash);
        String predecessorServeryKey = getPredecessorServer(serverHash);

        if (successorServerKey == null) {
            return Collections.emptyMap();
        }

        // Update successor start hash
        mappings.get(successorServerKey).setStartHash(mappings.get(predecessorServeryKey).getEndHash());
        return updateReadStartWithRemovedNode(serverHash);

//            // Update Replicas
//            hashToAddr.get(predecessorServeryKey).setReplica1(successorServerKey);
//            String prePredecessorServeryKey = getImmediatePredecessor(predecessorServeryKey).getKey();
//            hashToAddr.get(prePredecessorServeryKey).setReplica2(successorServerKey);
    }


    private Map<String, HashRange> updateReadStartWithRemovedNode(String removedHash) {
        Map<String, HashRange> ret = new HashMap<>();
        List<String> nextThreeSuccessors = getNextThreeSuccessors(removedHash);

        for (String successorServerKey:nextThreeSuccessors) {
            HashRange successorHashRange = mappings.get(successorServerKey);
            String oldReadStart = successorHashRange.getReadStartHash();
            String readStartServerKey;

            if (hashToAddr.containsKey(oldReadStart)) {
                readStartServerKey = hashToAddr.get(oldReadStart);
            } else {
                // Server associated with this hash died, get immediate successor
                readStartServerKey = getImmediateSuccessor(oldReadStart).getValue();
            }

            String newReadStartHash = mappings.get(readStartServerKey).getStartHash();
            successorHashRange.setReadStartHash(newReadStartHash);
            ret.put(successorServerKey, new HashRange(newReadStartHash, oldReadStart));
        }

        return ret;
    }

    private Map<String, HashRange> updateReadStartWithAddedNode(String addedHash) {
        Map<String, HashRange> ret = new HashMap<>();
        List<String> nextThreeSuccessors = getNextThreeSuccessors(addedHash);

        for (String successorServerKey:nextThreeSuccessors) {
            HashRange successorHashRange = mappings.get(successorServerKey);
            String oldReadStart = successorHashRange.getReadStartHash();
            String readStartServerKey = getImmediateSuccessor(oldReadStart).getValue();
            String newReadStartHash = mappings.get(readStartServerKey).getEndHash();
            successorHashRange.setReadStartHash(newReadStartHash);
            ret.put(successorServerKey, new HashRange(oldReadStart,newReadStartHash));
        }

        return ret;
    }

    private List<String> getNextThreeSuccessors(String hash) {
        Map.Entry<String, String> successor1 = getImmediateSuccessor(hash);
        Map.Entry<String, String> successor2 = getImmediateSuccessor(successor1.getKey());
        Map.Entry<String, String> successor3 = getImmediateSuccessor(successor2.getKey());
        return Arrays.asList(successor1.getValue(), successor2.getValue(), successor3.getValue());
    }

    /**
     * Retrieve an entrySet of mappings.
     *
     * @return entrySet of mappings between server addresses and respective HashRange
     */
    public Set<Map.Entry<String, HashRange>> entrySet() {
        return mappings.entrySet();
    }

    /**
     * Checks if given server exist in current metadata.
     *
     * @param hostName server name
     * @param port server port number
     * @return true if server has entry in metadata, false otherwise
     */
    public Boolean hasServer(String hostName, Integer port) {
        String hashKey = String.format("%s:%s", hostName, port);
        return mappings.containsKey(hashKey);
    }

    /**
     * Checks if given server exist in current metadata.
     *
     * @param addressAndPort server address and port number in the format of address:port
     * @return true if server has entry in metadata, false otherwise
     */
    public Boolean hasServer(String addressAndPort) {
        return mappings.containsKey(addressAndPort);
    }

    /**
     * Obtains the hash range of the server at given hostName and port.
     *
     * @param hostName server name
     * @param port server port number
     * @return HashRange for the given server
     */
    public HashRange getHashRange(String hostName, Integer port) {
        String hashKey = String.format("%s:%s", hostName, port);
        return mappings.get(hashKey);
    }

    /**
     * Obtains the hash range of the server at given hostName and port.
     *
     * @param addressAndPort server address and port number in the format of address:port
     * @return HashRange for the given server
     */
    public HashRange getHashRange(String addressAndPort) {
        return mappings.get(addressAndPort);
    }

    /**
     * Sets the HashRange of given server.
     *
     * @param hostName server address
     * @param port server port
     * @param range new HashRange for the server
     */
    public void setHashRange(String hostName, Integer port, HashRange range) {
        String serverKey = String.format("%s:%s", hostName, port);
        mappings.put(serverKey, range);
        hashToAddr.put(range.getEndHash(), serverKey);
    }

    /**
     * Sets the HashRange of given server.
     *
     * @param addressAndPort server address and port number in the format of address:port
     * @param range new HashRange for the server
     */
    public void setHashRange(String addressAndPort, HashRange range) {
        mappings.put(addressAndPort, range);
        hashToAddr.put(range.getEndHash(), addressAndPort);
    }


    /**
     * Serialize the current metadata class into string format.
     *
     * @return string representation of metadata
     */
    public String getSerializedForm() {
        return new Gson().toJson(mappings);
    }

    /**
     * Create new metadata class from a serialized string.
     *
     * @param serializedMap JSON serialized string of metadata
     * @return metadata based on input map
     */
    public static Metadata deserialize(String serializedMap) {
        Type type = new TypeToken<Map<String, HashRange>>(){}.getType();
        Map<String, HashRange> mappings = new Gson().fromJson(serializedMap, type);
        return new Metadata(mappings);
    }

//    public class Servers {
//        private String coordinator;
//
//        public void setReplica1(String replica1) {
//            this.replica1 = replica1;
//        }
//
//        public void setReplica2(String replica2) {
//            this.replica2 = replica2;
//        }
//
//        private String replica1;
//        private String replica2;
//
//        public String getReplica1() {
//            return replica1;
//        }
//
//        public String getReplica2() {
//            return replica2;
//        }
//
//        public String getCoordinator() {
//            return coordinator;
//        }
//
//        public void setCoordinator(String coordinator) {
//            this.coordinator = coordinator;
//        }
//
//        public Servers(String coordinator) {
//            this.coordinator = coordinator;
//        }
//
//        public List<String> getAll() {
//            return Arrays.asList(getCoordinator(), getReplica1(), getReplica2());
//        }
//    }

}
