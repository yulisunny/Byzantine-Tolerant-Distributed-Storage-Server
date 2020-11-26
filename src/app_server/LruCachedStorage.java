package app_server;

import common.messages.KVMessage;
import common.messages.Message;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A cache to store string based key value pairs with a least recently used eviction strategy.
 * <p>
 * Once the cache reaches max size, cached entries will be evicted with respect to the number of times accessed.
 * This was accomplished by using a {@link java.util.LinkedHashMap LinkedHashMap} which uses a doubly linked list to
 * keep track of access order of entries.
 * <p>
 * The cache utilize a {@link java.util.concurrent.locks.ReentrantLock ReentrantLock} to protect critical region.
 * ReentrantLock constructor accepts an optional fairness parameter. When set true, under contention,
 * locks favor granting access to the longest-waiting thread.
 */
public class LruCachedStorage extends AbstractCachedStorage {

    private final LinkedHashMap<String, String> cacheMap;
    private final int cacheSize;
    private ReentrantLock lock = new ReentrantLock(true);

    /**
     * Constructs a LruCachedStorage with a maximum size.
     *
     * @param size the maximum number of entries of key-value pair the cache can hold
     */
    public LruCachedStorage(int size, String hostname, int port) {
        super(hostname, port);
        cacheSize = size;
        cacheMap = new LinkedHashMap<String, String>(size, 0.75f, true) {

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return (size() > cacheSize);
            }
        };

    }

    /**
     * Inserts a key value pair into the cache with a write-through mechanism.
     *
     * @param key   the key that identifies the given value
     * @param value the value that is indexed by the given key
     * @return message indicating success/error. key value pair embedded in message for success put.
     */
    @Override
    public KVMessage put(String key, String value) {
        KVMessage rtn;
        //lock.lock();
        try {
            String previousValueOnDisk = persistToDisk(key, value);
            if (previousValueOnDisk == null) {
                // Key does not exist on disk.
                rtn = new Message(KVMessage.StatusType.PUT_SUCCESS, key, value, null);
            } else {
                // Key exists on disk.
                rtn = new Message(KVMessage.StatusType.PUT_UPDATE, key, value, null);
            }
            cacheMap.put(key, value);
        } catch (IOException e) {
            rtn = new Message(KVMessage.StatusType.PUT_ERROR, key, null, null);
        } finally {
            //lock.unlock();
        }
        return rtn;
    }

    /**
     * Retrieves the value indexed by the key from the cache.
     *
     * @param key the key that identifies a value
     * @return message indicating success/error. key value pair embedded in message for success get.
     *
     */
    @Override
    public KVMessage get(String key) {
        KVMessage rtn;
        lock.lock();
        try {
            String valueInCache = cacheMap.get(key);
            // Key exist in cache.
            if (valueInCache != null) {
                rtn = new Message(KVMessage.StatusType.GET_SUCCESS, key, valueInCache, null);
                // Key does not exist in cache.
            } else {
                String valueOnDisk = loadFromDisk(key);
                // Key exists on disk.
                if (valueOnDisk != null) {
                    rtn = new Message(KVMessage.StatusType.GET_SUCCESS, key, valueOnDisk, null);
                    cacheMap.put(key, valueOnDisk);
                    // Key does not exist on disk.
                } else {
                    rtn = new Message(KVMessage.StatusType.GET_ERROR, null, null, null);
                }
            }
        } catch (IOException e) {
            rtn = new Message(KVMessage.StatusType.GET_ERROR, key, null, null);
        } finally {
            lock.unlock();
        }
        return rtn;
    }

    /**
     * Removes the value indexed by the key from the cache.
     *
     * @param key key that identifies a value
     * @return message indicating success/error.
     */
    @Override
    public KVMessage delete(String key) {
        KVMessage rtn;
        //lock.lock();
        try {
            cacheMap.remove(key);
            String previousValueOnDisk = deleteFromDisk(key);
            if (previousValueOnDisk == null) {
                rtn = new Message(KVMessage.StatusType.DELETE_ERROR, key, null, null);
            } else {
                rtn = new Message(KVMessage.StatusType.DELETE_SUCCESS, key, null, null);
            }
        } catch (IOException e) {
            rtn = new Message(KVMessage.StatusType.DELETE_ERROR, key, null, null);
        } finally {
            //lock.unlock();
        }
        return rtn;
    }

    /**
     * Checks whether the given key has a key value pair in the cache.
     *
     * @param key key that identifies a value
     * @return boolean indicating whether a key exists in cache or not
     */
    public boolean containsKey(String key) {
        return cacheMap.containsKey(key);
    }
}
