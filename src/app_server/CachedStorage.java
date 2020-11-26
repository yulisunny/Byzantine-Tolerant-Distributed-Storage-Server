package app_server;

import common.messages.KVMessage;

/**
 * Storage mechanism to support quick lookup and access of a subset of key-value pairs.
 */
public interface CachedStorage {

    /**
     * Inserts the key value pair into cache if the key is not already in cache. Update the value of the pair if
     * the key already exists in cache. Persist to disk in any case.
     *
     * @param key   the key that identifies the given value
     * @param value the value that is indexed by the given key
     * @return message that confirms the insertion or an error
     */
    KVMessage put(String key, String value);

    /**
     * Retrieves the value for a given key from caching storage. First looks for the key in cache then on disk.
     *
     * @param key the key that identifies a value
     * @return message encapsulating the returned value or an error
     */
    KVMessage get(String key);

    /**
     * Removes the key value entry indexed by the input key from both cache and storage if exists.
     *
     * @param key key that identifies a value
     * @return message that confirms removal of an error
     */
    KVMessage delete(String key);
}
