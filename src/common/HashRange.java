package common;

import javax.xml.bind.ParseConversionEvent;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Class to keep track of starting and ending hex hashes for servers.
 */
public class HashRange implements Comparable<HashRange> {

    private String startHash;
    private String endHash;
    private String readStartHash;

    public HashRange(String start, String end) {
        startHash = start;
        endHash = end;
    }

    public HashRange(String start, String end, String readStart) {
        startHash = start;
        endHash = end;
        readStartHash = readStart;
    }

    public void setReadStartHash(String readStart) {
        this.readStartHash = readStart;
    }

    public String getReadStartHash() {
        return this.readStartHash;
    }

    /**
     * Gets the start hash in hex.
     *
     * @return hex representation of start hash
     */
    public String getStartHash() {
        return startHash;
    }

    /**
     * Sets the start hash.
     *
     * @param startHash hex representation of new start hash
     */
    public void setStartHash(String startHash) {
        this.startHash = startHash;
    }

    /**
     * Gets the end hash in hex.
     *
     * @return hex representation of end hash
     */
    public String getEndHash() {
        return endHash;
    }

    /**
     * Sets the end hash.
     *
     * @param endHash hex representation of new end hash
     */
    public void setEndHash(String endHash) {
        this.endHash = endHash;
    }

    @Override
    public int compareTo(HashRange o) {
        return startHash.compareTo(o.getStartHash());
    }

    /**
     * Checks whether the given hashed key falls within the range of current hash range.
     *
     * @param HashedKey Hex form of MD5 hash of some key
     * @return true if hashkey is valid for given hash range, false otherwise
     */
    public boolean isInRange(String HashedKey) {
        if (startHash.compareTo(endHash) == 0) {
            return true;
        }
        if (startHash.compareTo(endHash) < 0) {
            if (startHash.compareTo(HashedKey) < 0 && endHash.compareTo(HashedKey) >= 0) {
                return true;
            } else {
                return false;
            }
        }
        else {
            if (HashedKey.compareTo(startHash) > 0 || HashedKey.compareTo(endHash) <= 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean isInReadRange(String HashedKey) {
        if (readStartHash.compareTo(endHash) == 0) {
            return true;
        }
        if (readStartHash.compareTo(endHash) < 0) {
            if (readStartHash.compareTo(HashedKey) < 0 && endHash.compareTo(HashedKey) >= 0) {
                return true;
            } else {
                return false;
            }
        }
        else {
            if (HashedKey.compareTo(readStartHash) > 0 || HashedKey.compareTo(endHash) <= 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Computes hex representation of MD5 hash for the given input string.
     *
     * @param input input to be used for md5 hash
     * @return hex string of md5 hash of input
     */
    public static String getMd5Hash(String input) {
        byte[] bytesOfMessage = input.getBytes();
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
        String hex = (new HexBinaryAdapter()).marshal(md.digest(bytesOfMessage));
        return hex;
    }
}
