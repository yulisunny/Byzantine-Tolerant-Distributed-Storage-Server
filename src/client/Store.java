package client;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.HashRange;
import common.Metadata;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * APIs for all communication to the storage server using the {@link CommInterface CommInterface}.
 *
 * @see CommInterface
 * @see common.messages.KVMessage
 */
public class Store implements CommInterface {

    private static int MAX_KEY_LENGTH = 200;
    private static int MAX_VALUE_LENGTH = 120000;

    private static Logger logger = Logger.getRootLogger();

    private Metadata metadata;
    private SocketStreams serverSocket;
    private String serverKey;
    private String address;
    private int port;

    private static List<Pair<String, Pair<String, String>>> putRequestsSent;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    /**
     * Initialize Store with address and port of an arbitrary Server
     *
     * @param address the address of the Server
     * @param port    the port of the Server
     */
    public Store(String address, int port) {
        this.address = address;
        this.port = port;
        this.serverKey = String.format("%s:%d", this.address, this.port);
        putRequestsSent = new ArrayList<>();
    }

    /**
     * Establishes a connection to the Server. Creates socket, input and output byte streams
     * to be needed for subsequent communication to/from the server.
     *
     * @throws IOException              socked I/O based failures such as server disconnection
     * @throws SecurityException        if a security manager exists and its
     *                                  {@code checkConnect} method doesn't allow the operation
     * @throws IllegalArgumentException Invalid port number
     */
    @Override
    public void connect() throws Exception {
        this.metadata = new Metadata();
        serverSocket = new SocketStreams(new Socket(address, port));
        String serverKey = String.format("%s:%d", address, port);
        metadata.addNewServer(serverKey, HashRange.getMd5Hash(serverKey));
        logger.info("Connection established");
    }

    /**
     * Disconnects from the currently connected Server. Closing of socket, input and output
     * byte streams.
     */
    @Override
    public void disconnect() {
        try {
            serverSocket.getInputStream().close();
            serverSocket.getOutputStream().close();
            serverSocket.getServerSocket().close();
        } catch (IOException e) {
            logger.error(String.format("Error disconnecting with host %s and port %d", address, port), e);
        }
    }

    /**
     * Inserts a key-value pair into the Server.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return message indicating the status and results of the request
     * @throws IllegalArgumentException key or value exceeded allowed length
     * @throws IOException              needs to establish connection to a valid Server before request
     */
    @Override
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

        String listingAddrPort = null;
        try {
            listingAddrPort = serverSocket.getLocalAddress() + ":" + ClientListenRunnable.getPortNumber();
        } catch (Exception e) {
            listingAddrPort = "127.0.0.1:1";
        }
        KVMessage message = new Message(KVMessage.StatusType.PUT, key, value, listingAddrPort);
        while (true) {
            // Make sure we know at least one server
            if (metadata.size() < 1) {
                throw new IllegalStateException("All servers known are down. Please reconnect with another server. ");
            }

            // Get the corresponding server that current hash key is mapped to
            SocketStreams serverSocketStreams = getSocketStreams(key);

            try {
                logger.info(String.format("Sending the request to server %s", serverKey));
                logger.debug(String.format("Key has a hash of %s", HashRange.getMd5Hash(key)));
                Pair<String, String> keyValue = new ImmutablePair<>(key, value);
                putRequestsSent.add(new ImmutablePair<>(serverKey, keyValue));
                sendMessage(message, serverSocketStreams.getOutputStream());
                logger.info(String.format("Sent PUT reply with msg %s", message.toString()));
            } catch (Exception e) {
                logger.info("Encountered error while sending requests. Attempt reconnect with another server.");
                metadata.removeServer(serverKey);
                continue;
            }

            KVMessage response = receiveMessage(serverSocketStreams.getInputStream());
            logger.info(String.format("Received PUT reply with msg %s", response.toString()));

            if (response.getStatus() != KVMessage.StatusType.PUT_SUCCESS && response.getStatus() != KVMessage.StatusType.PUT_UPDATE && response.getStatus() != KVMessage.StatusType.DELETE_SUCCESS) {
                putRequestsSent.remove(putRequestsSent.size() - 1);
            }

            KVMessage.StatusType status = response.getStatus();
            if (status.equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                logger.info("Updating metadata cached.");
                String serializedMetadata = response.getMetadata();
                updateClientMetadata(Metadata.deserialize(serializedMetadata));
            } else if (status.equals(KVMessage.StatusType.SERVER_WRITE_LOCK)) {
                logger.info("Server busy, retrying in 1000 ms");
                Thread.sleep(1000);
            } else {
                return response;
            }
        }
    }

    /**
     * Retrieves the value for the given key from the Server.
     *
     * @param key the key that identifies the value.
     * @return message indicating the status and results of the reques
     * @throws IOException needs to establish connection to a valid Server before request
     */
    @Override
    public KVMessage get(String key) throws Exception {
        if (!isConnected()) {
            throw new IOException("Not connected to any server");
        }

        KVMessage message = new Message(KVMessage.StatusType.GET, key, null, null);
        while (true) {
            // Make sure we know at least one server
            if (metadata.size() < 1) {
                throw new IllegalStateException("All servers known are down. Please reconnect with another server. ");
            }

            // Get the corresponding server that current hash key is mapped to
            SocketStreams serverSocketStreams = getSocketStreamsForRead(key);
            KVMessage response = null;
            try {
                sendMessage(message, serverSocketStreams.getOutputStream());
                logger.info(String.format("Sent GET request with msg %s", message.getSerializedMsg()));
                response = receiveMessage(serverSocketStreams.getInputStream());
                logger.info(String.format("Received GET reply with msg %s", response));
            } catch (Exception e) {
                logger.info("Encountered error while sending requests. Attempt reconnect with another server.");
                metadata.removeServer(serverKey);
                continue;
            }

            KVMessage.StatusType status = response.getStatus();
            if (status.equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                String serializedMetadata = response.getMetadata();
                logger.info("Updating metadata");
                updateClientMetadata(Metadata.deserialize(serializedMetadata));
            } else if (status.equals(KVMessage.StatusType.SERVER_WRITE_LOCK)) {
                logger.info("Server write lock, retrying in 1000 ms.");
                Thread.sleep(1000);
            } else {
                List<String> backups = this.metadata.getBackups(HashRange.getMd5Hash(key));
                String previousServerKey = this.serverKey;
                for (String serverKey : backups) {
                    new ClientGetIntegrityCheck(previousServerKey, serverKey, response.getKey(), response.getValue()).start();
                }
                return response;
            }
        }

    }

    @Override
    public boolean subscribe(String key) {
        if (!isConnected()) {
            logger.info("Cannot subscribe because not logged in.");
            return false;
        }

        String listingAddrPort = serverSocket.getLocalAddress() + ":" + ClientListenRunnable.getPortNumber();
        KVMessage message = new Message(KVMessage.StatusType.SUBSCRIBE, key, null, listingAddrPort);

        while (true) {
            // Make sure we know at least one server
            if (metadata.size() < 1) {
                return false;
            }

            try {
                // Get the corresponding server that current hash key is mapped to
                SocketStreams serverSocketStreams = getSocketStreams(key);
                sendMessage(message, serverSocketStreams.getOutputStream());
                KVMessage response = receiveMessage(serverSocketStreams.getInputStream());
                logger.info(String.format("Received SUBSCRIBE reply with msg %s", response.toString()));
                KVMessage.StatusType status = response.getStatus();
                if (status.equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                    logger.info("Updating metadata cached.");
                    String serializedMetadata = response.getMetadata();
                    updateClientMetadata(Metadata.deserialize(serializedMetadata));
                } else if (status.equals(KVMessage.StatusType.SERVER_WRITE_LOCK)) {
                    logger.info("Server busy, retrying in 1000 ms");
                    Thread.sleep(1000);
                } else {
                    return true;
                }
            } catch (Exception e) {
                logger.info("Encountered error while sending subscription requests. Attempt reconnect with another server.");
                metadata.removeServer(serverKey);
                continue;
            }
        }
    }

    @Override
    public boolean unsubscribe(String key) {
        if (!isConnected()) {
            logger.info("Cannot unsubscribe because not logged in.");
            return false;
        }

        String listingAddrPort = serverSocket.getLocalAddress() + ":" + ClientListenRunnable.getPortNumber();
        KVMessage message = new Message(KVMessage.StatusType.UNSUBSCRIBE, key, null, listingAddrPort);


        if (metadata.size() < 1) {
            return false;
        }

        try{
            sendMessage(message, serverSocket.getOutputStream());
        } catch (IOException e) {
            logger.error("Unsubscribe has trouble sending to server " + serverSocket.getPort(), e);
            return false;
        }
        return true;
    }

    public static String checkPutSent(String serverKey, String key) {
        for (int i = putRequestsSent.size() - 1; i >= 0; i--) {
            Pair<String, Pair<String, String>> pair = putRequestsSent.get(i);
            if (pair.getKey().equals(serverKey) && pair.getValue().getKey().equals(key)) {
                return pair.getValue().getValue();
            }
        }
        return null;
    }

    private void updateClientMetadata(Metadata newMetadata) throws IOException {
        this.metadata = newMetadata;

        // Close socket streams for servers that no longer exist
        if (!this.metadata.hasServer(this.serverKey)) {
            try {
                String newServerKey = this.metadata.getImmediateSuccessor(HashRange.getMd5Hash(this.serverKey)).getValue();
                setNewServerSocket(newServerKey);
                this.serverKey = newServerKey;
            } catch (IOException e) {
                logger.error("Failed to close socket streams.");
            }
        }
    }

    private SocketStreams getSocketStreams(String key) throws IOException {
        String hashedKey = HashRange.getMd5Hash(key);
        String serverKey = metadata.getSuccessorServer(hashedKey);

        // Wanted server is the same as whats currently cached
        if (serverKey.equals(this.serverKey)) {
            return serverSocket;
        }

        // Close existing and open new socket
        setNewServerSocket(serverKey);
        return this.serverSocket;
    }

    private SocketStreams getSocketStreamsForRead(String key) throws IOException {
        String hashedKey = HashRange.getMd5Hash(key);

        List<String> serverKeys = metadata.getReadableServers(hashedKey);

        // Lazily load existing socket
        if (serverKeys.contains(this.serverKey)) {
            logger.debug(String.format("Can read data from %s, picking %s.", serverKeys, serverKey));
            return serverSocket;
        }

        String newServerKey = serverKeys.get(new Random().nextInt(serverKeys.size()));
        logger.debug(String.format("Can read data from %s, picking %s.", serverKeys, newServerKey));

        // Close existing and open new socket
        setNewServerSocket(newServerKey);
        return this.serverSocket;
    }

    private void setNewServerSocket(String serverKey) throws IOException {
        serverSocket.getInputStream().close();
        serverSocket.getOutputStream().close();
        serverSocket.getServerSocket().close();
        logger.info("Closing server sockets for server " + this.serverKey);

        String[] splits = serverKey.split(":");
        String address = splits[0];
        Integer port = Integer.valueOf(splits[1]);
        this.serverKey = serverKey;
        this.serverSocket = new SocketStreams(new Socket(address, port));
        this.address = address;
        this.port = port;
        logger.info("Setting up new sockets for server " + serverKey);
    }

    /**
     * Checks if there is a valid socket, input and output stream specified. Part of the communication module.
     *
     * @return boolean indicating the result of the check
     */
    private boolean isConnected() {
        return serverSocket != null;
    }

    /**
     * Serializes and sends the request message to the Server. Part of the communication module.
     *
     * @param msg message of the request
     * @throws IOException if the output stream is closed during message writes
     */
    private void sendMessage(KVMessage msg, OutputStream output) throws IOException {
        byte[] msgBytes = msg.getBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message:\t '" + msg.getSerializedMsg() + "'");
    }

    /**
     * Receives and deserializes the reply message from the Server. Part of the communication module.
     *
     * @return server reply message
     * @throws IOException if the input stream is closed when receiving message
     */
    private KVMessage receiveMessage(InputStream input) throws IOException {

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

        while (read != 10 && reading) {
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
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
            if ((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

			/* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            readInt = input.read();
            if (readInt == -1) {
                throw new IOException("Connection dead");
            }
            read = (byte) readInt;
        }

        if (msgBytes == null) {
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

    private class SocketStreams {

        private Socket serverSocket;
        private OutputStream outputStream;
        private InputStream inputStream;

        SocketStreams(Socket socket) {
            serverSocket = socket;
            try {
                outputStream = socket.getOutputStream();
                inputStream = socket.getInputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Socket getServerSocket() {
            return serverSocket;
        }

        public OutputStream getOutputStream() {
            return outputStream;
        }

        public InputStream getInputStream() {
            return inputStream;
        }

        public String getLocalAddress() {
            return serverSocket.getLocalAddress().getHostAddress();
        }

        public int getPort() {
            return serverSocket.getPort();
        }

    }


}
