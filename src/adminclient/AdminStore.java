package adminclient;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.Metadata;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class AdminStore implements AdminCommInterface {

    private static int MAX_KEY_LENGTH = 20;
    private static int MAX_VALUE_LENGTH = 120000;

    private Logger logger = Logger.getRootLogger();

    private String address;
    private int port;
    private Socket serverSocket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    /**
     * Initialize AdminStore with address and port of Server
     *
     * @param address the address of the KVAdminServer
     * @param port the port of the KVAdminServer
     */

    public AdminStore(String address, int port){
        this.address = address;
        this.port = port;
    }

    @Override
    public void initKVServer(int cacheSize, String replacementStrategy, Metadata metadata) throws Exception {
        connect();
        if (!isConnected()) {
            throw new IOException("Not connected to the server");
        }
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_INIT_KVSERVER, String.valueOf(cacheSize), replacementStrategy, metadata.getSerializedForm());
        sendMessage(message);
        //KVMessage response = receiveMessage();
    }

    @Override
    public void start() throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_START, null, null, null);
        sendMessage(message);
    }

    @Override
    public void stop() throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_STOP, null, null, null);
        sendMessage(message);
    }

    @Override
    public void shutDown() throws Exception{
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_SHUTDOWN, null, null, null);
        sendMessage(message);
        if (serverSocket != null) {
            input.close();
            output.close();
            serverSocket.close();
        }
    }

    @Override
    public void lockWrite() throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_LOCKWRITE, null, null, null);
        sendMessage(message);
    }

    @Override
    public void unLockWrite() throws Exception{
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_UNLOCKWRITE, null, null, null);
        sendMessage(message);
    }

    @Override
    public void moveData(String host, String port, Metadata metadataRangeInfo) throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_MOVEDATA, host, port, metadataRangeInfo.getSerializedForm());
        sendMessage(message);
        KVMessage response = receiveMessage();
    }

    @Override
    public void copyData(String host, String port, Metadata metadataRangeInfo) throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_COPYDATA, host, port, metadataRangeInfo.getSerializedForm());
        sendMessage(message);
        KVMessage response = receiveMessage();
    }

    @Override
    public void deleteData() throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_DELETEDATA, null, null, null);
        sendMessage(message);
        KVMessage response = receiveMessage();
    }

    @Override
    public void update(Metadata metadata) throws Exception {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_UPDATE, null, null, metadata.getSerializedForm());
        sendMessage(message);
    }

    @Override
    public void pin() throws IOException {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_PIN, null, null, null);
        sendMessage(message);
        KVMessage response = receiveMessage();
    }

    @Override
    public void unsubscribe(String key, String value) throws IOException {
        KVMessage message = new Message(KVMessage.StatusType.ADMIN_UNSUBSCRIBE, key, value, null);
        sendMessage(message);
    }

    /**
     * Establishes a connection to the Server. Creates socket, input and output byte streams
     * to be needed for subsequent communication to/from the server.
     *
     * @throws IOException socked I/O based failures such as server disconnection
     * @throws SecurityException if a security manager exists and its
     *             {@code checkConnect} method doesn't allow the operation
     * @throws IllegalArgumentException Invalid port number
     */
    @Override
    public void connect() throws Exception {
        serverSocket = new Socket(address, port);
        serverSocket.setSoTimeout(5 * 1000);
        output = serverSocket.getOutputStream();
        input = serverSocket.getInputStream();
        logger.info("Connection established");
    }

    /**
     * Disconnects from the currently connected Server. Closing of socket, input and output
     * byte streams.
     */
    public void disconnect() {
        if (serverSocket != null) {
            try {
                output.close();
                input.close();
                serverSocket.close();
                serverSocket = null;
            } catch (IOException e) {
                logger.error(String.format("Error disconnecting with host %s and port %d", address, port), e);
            }
        }
    }

    /**
     * Inserts a key-value pair into the Server.
     *
     * @param key
     *            the key that identifies the given value.
     * @param value
     *            the value that is indexed by the given key.
     * @return message indicating the status and results of the request
     * @throws IllegalArgumentException key or value exceeded allowed length
     * @throws IOException needs to establish connection to a valid Server before request
     */
    public KVMessage put(String key, String value) throws Exception {
        if (!isConnected()) {
            throw new IOException("Not connected to any server");
        }

        if (key.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("Key length exceeded " + MAX_KEY_LENGTH);
        }

        if (value != null && value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("Value length exceeded " + MAX_VALUE_LENGTH);
        }

        KVMessage message = new Message(KVMessage.StatusType.PUT, key, value, null);
        sendMessage(message);
        logger.info(String.format("Sent PUT reply with msg %s", message.toString()));

        KVMessage response = receiveMessage();
        logger.info(String.format("Received PUT reply with msg %s", response.toString()));
        return response;
    }

    /**
     * Retrieves the value for the given key from the Server.
     *
     * @param key
     *            the key that identifies the value.
     * @return message indicating the status and results of the request
     * @throws IOException needs to establish connection to a valid Server before request
     */
    public KVMessage get(String key) throws Exception {
        if (!isConnected()) {
            throw new IOException("Not connected to any server");
        }
        KVMessage message = new Message(KVMessage.StatusType.GET, key, null, null);
        sendMessage(message);
        logger.info(String.format("Sent GET request with msg %s", message.getSerializedMsg()));

        KVMessage response = receiveMessage();
        logger.info(String.format("Received GET reply with msg %s", response));
        return response;
    }

    /**
     * Checks if there is a valid socket, input and output stream specified. Part of the communication module.
     *
     * @return boolean indicating the result of the check
     */
    private boolean isConnected() {
        return serverSocket != null && input != null && output != null;
    }

    /**
     * Serializes and sends the request message to the Server. Part of the communication module.
     *
     * @param msg message of the request
     * @throws IOException if the output stream is closed during message writes
     */
    private void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getBytes();
        try {
            output.write(msgBytes, 0, msgBytes.length);
        } catch (Exception e) {
            logger.debug("Caught exception in AdminStore send message!", e);
        }
        output.flush();
        logger.info("Send message:\t '" + msg.getSerializedMsg() + "'");
    }
    /**
     * Receives and deserializes the reply message from the Server. Part of the communication module.
     *
     * @return server reply message
     * @throws IOException if the input stream is closed when receiving message
     */
    private KVMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        int readInt = input.read();
        if (readInt == -1) {
            throw new IOException("Connection dead");
        }
        byte read = (byte) readInt;
        boolean reading = true;

        while(read != 10 && reading) {
			/* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

			/* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

			/* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            readInt = input.read();
            if (readInt == -1) {
                throw new IOException("Connection dead");
            }
            read = (byte) readInt;
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        // Deserialize the received bytes using CSV parser
        msgBytes = tmp;
        String rawMessage = new String(msgBytes, "US-ASCII");
        JsonObject jsonObject = new JsonParser().parse(rawMessage).getAsJsonObject();
        String arg1 = jsonObject.get("arg1").getAsString();
        String arg2 = jsonObject.get("arg2").getAsString();
        String arg3 = jsonObject.get("arg3").getAsString();
        KVMessage.StatusType statusType = KVMessage.StatusType.valueOf(jsonObject.get("statusType").getAsString());
        KVMessage message = new Message(statusType, arg1, arg2, arg3);
        return message;
    }
}
