package app_server;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientIntegrityCheckRunnable implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket destinationSocket;
    private InputStream input;
    private OutputStream output;
    private String key;
    private String value;
    private String serverKey;
    private boolean serverCompromised = false;
    private Socket adminSocket;
    private String adminAddress;
    private KVMessage.StatusType statusType;
    private String ownAddressAndPort;

    public ClientIntegrityCheckRunnable(Socket destinationSocket,
                                        String key,
                                        String value,
                                        String serverKey,
                                        boolean serverCompromised,
                                        KVMessage.StatusType statusType,
                                        String adminAddress,
                                        String ownAddressAndPort) {

        this.destinationSocket = destinationSocket;
        this.key = key;
        this.value = value;
        this.serverKey = serverKey;
        this.serverCompromised = serverCompromised;
        this.adminAddress = adminAddress;
        this.statusType = statusType;
        this.ownAddressAndPort = ownAddressAndPort;
    }

    public void run() {

        if (!serverCompromised) {
            try {
                if (destinationSocket != null) {
                    output = destinationSocket.getOutputStream();
                    input = destinationSocket.getInputStream();
                }
                if (statusType == KVMessage.StatusType.PUT) {
                    KVMessage request = new Message(KVMessage.StatusType.CLIENT_PUT_INTEGRITY_CHECK, key, value, serverKey+","+ownAddressAndPort);
                    sendMessage(request);
                }
                else if (statusType == KVMessage.StatusType.GET) {
                    KVMessage request = new Message(KVMessage.StatusType.CLIENT_TRUE_GET, key, value, null);
                    sendMessage(request);
                }
                else if (statusType == KVMessage.StatusType.UNSUBSCRIBE) {
                    this.adminSocket = new Socket(adminAddress, 61034);
                    output = adminSocket.getOutputStream();
                    input = adminSocket.getInputStream();

                    KVMessage requestToAdmin = new Message(KVMessage.StatusType.SERVER_BROADCAST_UNSUBSCRIBE, key, value, null);
                    sendMessage(requestToAdmin);
                    closeAdminConnection();
                }
                close();
            } catch (Exception e) {
                logger.error("Did not reach client server", e);
            }
        } else {
            logger.debug("server: " + serverKey + " IS COMPROMISED!");
            // server is dead, time to inform admin
            try {
                this.adminSocket = new Socket(adminAddress, 61034);
                output = adminSocket.getOutputStream();
                input = adminSocket.getInputStream();
                KVMessage requestToAdmin = new Message(KVMessage.StatusType.ADMIN_SERVER_COMPROMISED, serverKey, null, null);
                sendMessage(requestToAdmin);
                closeAdminConnection();
            } catch (IOException e) {
                logger.debug("Admin SOCKET CONNECTION FAILED!!" + e);
            }
        }
    }

    public void close() throws IOException {
        try {
            output.close();
        }
        finally {
            try {
                input.close();
            }
            finally {
                destinationSocket.close();
            }
        }
    }

    private void closeAdminConnection() throws IOException {
        try {
            output.close();
        }
        finally {
            try {
                input.close();
            }
            finally {
                destinationSocket.close();
            }
        }
    }

    /**
     * Method sends a KVMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + destinationSocket.getInetAddress().getHostAddress() + ":"
                + destinationSocket.getPort() + ">: '"
                + msg.getSerializedMsg() +"'");
    }

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

        if (read == -1){
            return null;
        }
        while(read != 10 && reading) {/* CR, LF, error */
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

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

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

        // Deserialize
        msgBytes = tmp;
        String rawMessage = new String(msgBytes, "US-ASCII");


        JsonObject jsonObject = new JsonParser().parse(rawMessage).getAsJsonObject();
        String arg1= jsonObject.get("arg1").getAsString();
        String arg2 = jsonObject.get("arg2").getAsString();
        KVMessage.StatusType statusType = KVMessage.StatusType.valueOf(jsonObject.get("statusType").getAsString());
        String arg3 = jsonObject.get("arg3").getAsString();
        KVMessage message = new Message(statusType, arg1, arg2, arg3);

        logger.info("RECEIVE \t<"
                + destinationSocket.getInetAddress().getHostAddress() + ":"
                + destinationSocket.getPort() + ">: '"
                + message.getSerializedMsg() + "'");
        return message;
    }
}
