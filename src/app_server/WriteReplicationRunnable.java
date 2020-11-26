package app_server;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.HashRange;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class WriteReplicationRunnable implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket destinationSocket;
    private InputStream input;
    private OutputStream output;
    private HashRange range;
    private String key;
    private String value;
    private String serverKey;
    private String clientKey;

    public WriteReplicationRunnable(Socket destinationSocket, String key, String value, String serverKey, String clientKey) {
        this.destinationSocket = destinationSocket;
        this.isOpen = true;
        this.range = range;
        this.key = key;
        this.value = value;
        this.serverKey = serverKey;
        this.clientKey = clientKey;
    }

    public void run() {
        try {
            output = destinationSocket.getOutputStream();
            input = destinationSocket.getInputStream();
            // need to add in a serverKey$clientKey metadata field for ADMIN_REPLICATION, waiting for client implementations
            String serverAndClientKey = serverKey + "," + clientKey;
            KVMessage request = new Message(KVMessage.StatusType.ADMIN_REPLICATION, key, value, serverAndClientKey);
            sendMessage(request);

//            KVMessage replyMsg = new Message(KVMessage.StatusType.ADMIN_FILETRANSFER_COMPLETE, null, null, null);
//            sendMessage(request);
//            KVMessage message = receiveMessage();
        } catch (Exception e) {

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