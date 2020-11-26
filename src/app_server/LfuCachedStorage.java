package app_server;

import common.messages.KVMessage;
import common.messages.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A cache to store string based key value pairs with a least frequently used eviction strategy.
 * <p>
 * Once the cache reaches max size, regardless of the number of times accessed, the cached entry that was used least
 * frequently will be evicted first. The strategy was accomplished by keeping track of key-value pairs using a frequency
 * list and a key-value pair map.
 * <p>
 * To improve performance for clustered high frequent keys, an additional map was used to cache the frequencies of
 * some keys with high number of accesses.
 * <p>
 * The cache utilize a {@link java.util.concurrent.locks.ReentrantLock ReentrantLock} to protect critical region.
 * ReentrantLock constructor accepts an optional fairness parameter. When set true, under contention,
 * locks favor granting access to the longest-waiting thread.
 */
public class LfuCachedStorage extends AbstractCachedStorage {

    private final HashMap<String, Entry> cacheMap;
    private ReentrantLock lock = new ReentrantLock(true);
    private List<List<String>> freqList;
    private HashMap<String, Integer> highFreqEntryMap;
    private final int freqListSize;
    private final int cacheSize;

    /**
     * Constructs a LfuCachedStorage with a maximum cache size and a maximum frequency size.
     *
     * @param size the maximum number of entries of key-value pair the cache holds
     */
    public LfuCachedStorage(int size, String hostname, int port) {
        super(hostname, port);
        cacheSize = size;
        cacheMap = new HashMap<>(cacheSize);
        freqList = new ArrayList<List<String>>();
        freqList.add(0, new ArrayList<String>());
        highFreqEntryMap = new HashMap<>();
        freqListSize = size; // Make the default size of the freqList to be the same size as the cache
    }

    /**
     * Constructs a LfuCachedStorage with a maximum cache size and a maximum frequency size.
     *
     * @param size the maximum number of entries of key-value pair the cache holds
     * @param fListSize the maximum frequency the cache will keep track of
     */
//    public LfuCachedStorage(int size, int fListSize) {
//        cacheSize = size;
//        cacheMap = new HashMap<>(cacheSize);
//        freqList = new ArrayList<List<String>>();
//        freqList.add(0, new ArrayList<String>());
//        highFreqEntryMap = new HashMap<>();
//        freqListSize = fListSize;
//    }

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

            Entry entryInCache = cacheMap.get(key);
            if (entryInCache == null) {
                // Entry not in cache
                if (cacheMap.size() >= cacheSize) {
                    evictLeastFrequentlyUsedEntry();
                }
                entryInCache = new Entry(value);
                freqList.get(0).add(key);
            } else {
                // Entry in cache, update frequency lists
                if (entryInCache.frequency < freqListSize) {
                    freqList.get(entryInCache.frequency).remove(key);
                    entryInCache.frequency = entryInCache.frequency + 1;
                    entryInCache.value = value;
                    if (freqList.size() - 1 < entryInCache.frequency) {
                        freqList.add(entryInCache.frequency, new ArrayList<String>());
                    }
                    freqList.get(entryInCache.frequency).add(key);
                } else {
                    if (entryInCache.frequency == freqListSize) {
                        freqList.get(entryInCache.frequency).remove(key);
                    }
                    entryInCache.frequency = entryInCache.frequency + 1;
                    entryInCache.value = value;
                    highFreqEntryMap.put(key, entryInCache.frequency);
                }
            }
            cacheMap.put(key, entryInCache);
        } catch (IOException e) {
            rtn = new Message(KVMessage.StatusType.PUT_ERROR, key, value, null);
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
            Entry entryInCache = cacheMap.get(key);
            if (entryInCache != null) {
                if (entryInCache.frequency < freqListSize) {
                    freqList.get(entryInCache.frequency).remove(key);
                    entryInCache.frequency = entryInCache.frequency + 1;
                    if (freqList.size() - 1 < entryInCache.frequency) {
                        freqList.add(entryInCache.frequency, new ArrayList<String>());
                    }
                    freqList.get(entryInCache.frequency).add(key);
                } else {
                    if (entryInCache.frequency == freqListSize) {
                        freqList.get(entryInCache.frequency).remove(key);
                    }
                    entryInCache.frequency = entryInCache.frequency + 1;
                    highFreqEntryMap.put(key, entryInCache.frequency);
                }
                cacheMap.put(key, entryInCache);
                rtn = new Message(KVMessage.StatusType.GET_SUCCESS, key, entryInCache.value, null);
            } else {
                try {
                    String valueOnDisk = loadFromDisk(key);
                    Entry entryOnDisk = new Entry(valueOnDisk);
                    if (valueOnDisk != null) {
                        if (cacheMap.size() >= cacheSize) {
                            evictLeastFrequentlyUsedEntry();
                        }
                        rtn = new Message(KVMessage.StatusType.GET_SUCCESS, key, valueOnDisk, null);
                        cacheMap.put(key, entryOnDisk);
                        freqList.get(0).add(key);
                        // Key does not exist on disk.
                    } else {
                        rtn = new Message(KVMessage.StatusType.GET_ERROR, key, null, null);
                    }
                } catch (IOException e) {
                    rtn = new Message(KVMessage.StatusType.GET_ERROR, key, null, null);
                }
            }
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
            Entry deletedEntry = cacheMap.remove(key);
            // remove it from frequency list: arraylist returns false when it removes non-existent key
            if (deletedEntry != null) {
                if (deletedEntry.frequency <= freqListSize) {
                    freqList.get(deletedEntry.frequency).remove(key);
                } else {
                    highFreqEntryMap.remove(key);
                }
            }

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
     * Removes the key-value pair that has the fewest number of access from cache.
     */
    private void evictLeastFrequentlyUsedEntry() {
        boolean freqListIsEmpty = true;
        for (int i = 0; i < freqList.size(); i++) {
            if (freqList.get(i) != null && !freqList.get(i).isEmpty()) {
                String key = freqList.get(i).remove(0);
                cacheMap.remove(key);
                freqListIsEmpty = false;
                break;
            }
        }
        if (freqListIsEmpty) {
            int min = 0;
            String keyToEvict = null;
            for (Map.Entry<String, Integer> entry : highFreqEntryMap.entrySet()) {
                if (min == 0 || min > entry.getValue()) {
                    min = entry.getValue();
                    keyToEvict = entry.getKey();
                }
            }
            if (keyToEvict != null) {
                highFreqEntryMap.remove(keyToEvict);
                cacheMap.remove(keyToEvict);
            }
        }
    }

    private class Entry {
        private String value;
        private int frequency;

        public Entry(String pValue) {
            value = pValue;
            frequency = 0;
        }
    }

    // The following functions were only used in testing.
    /**
     * Checks whether the given key has a key value pair in the cache.
     *
     * @param key key that identifies a value
     * @return boolean indicating whether a key exists in cache or not
     */
    public boolean containsKey(String key) {
        return cacheMap.containsKey(key);
    }

    /**
     * Obtains the current size of the cache.
     *
     * @return current size of cache
     */
    public int getCacheSize() {
        return cacheMap.size();
    }

    /**
     * Obtains the key at a specified position of a frequency list.
     *
     * @param frequency number of times a key was accessed
     * @param position index of the frequency list
     * @return key that identifies some key-value pair with a given frequency
     */
    public String getKeyFromFreqList(int frequency, int position) {
        return freqList.get(frequency).get(position);
    }

    /**
     * Obtains the number of times a key has been accessed if it has high access frequency.
     *
     * @param key key that identifies some key-value pair with a high access frequency
     * @return the number of times the key has been accessed or -1 if it is not a high frequency key
     */
    public int getFreqFromHighFreqEntryMap(String key) {
        if (highFreqEntryMap.containsKey(key)) {
            return highFreqEntryMap.get(key);
        }
        return -1;
    }
}
