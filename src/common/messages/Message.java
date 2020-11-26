package common.messages;


import org.apache.commons.lang3.StringEscapeUtils;


public class Message implements KVMessage {

    private static final char LINE_FEED = 0x0A;
    private static final char CARRIAGE_RETURN = 0x0D;

    private StatusType status;
    private String key;
    private String value;
    private String metadata;
    private String serializedMsg;
    private byte[] bytes;

    public Message(StatusType pStatus, String pKey, String pValue, String pMetadata) {
        status = pStatus;
        key = pKey;
        value = pValue;
        metadata = pMetadata;
        serializedMsg = serialize(pStatus, pKey, pValue, pMetadata);
        bytes = toByteArray(serializedMsg);
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getMetadata() { return metadata; }

    @Override
    /**
     * Returns an array of bytes that represent the ASCII coded message content.
     *
     * @return the content of this message as an array of bytes
     * 		in ASCII coding.
     */
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public String getSerializedMsg() {
        return serializedMsg;
    }

    @Override
    public String toString() {
        return serializedMsg;
    }

    private String serialize(StatusType statusType, String key, String value, String metadata) {
        String escapedKey = StringEscapeUtils.escapeJson(key);
        String escapedValue = StringEscapeUtils.escapeJson(value);
        String escapedMetadata = StringEscapeUtils.escapeJson(metadata);
        String serialized_string = "{\"statusType\":" + "\"" + statusType.toString()+ "\"," +
                "\"arg1\":" + "\"" + escapedKey + "\"," + "\"arg2\":" + "\"" + escapedValue + "\","
                + "\"arg3\":" + "\"" + escapedMetadata + "\"}";
        return serialized_string;
    }

    private byte[] toByteArray(String s){
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }
}
