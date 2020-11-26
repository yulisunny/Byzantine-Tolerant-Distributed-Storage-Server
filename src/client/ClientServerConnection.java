package client;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientServerConnection implements Runnable {

    private Logger logger = Logger.getRootLogger();

    private boolean isOpen = true;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public Socket socket;
    private InputStream input;
    private OutputStream output;

    public ClientServerConnection(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            output = socket.getOutputStream();
            input = socket.getInputStream();

            while(isOpen) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    closeConnection();
                    if (latestMsg == null){
                        logger.info("Connection lost with server, shutting down");
                        isOpen = false;
                    }
                    else if (latestMsg.getStatus() == KVMessage.StatusType.CLIENT_PUT_INTEGRITY_CHECK) {
                        String maybeCompromisedServerThisServer = latestMsg.getMetadata();
                        String [] array = maybeCompromisedServerThisServer.split(",");
                        String maybeCompromisedServer = array[0];
                        String thisServer = array[1];
                        String value = Store.checkPutSent(maybeCompromisedServer, latestMsg.getKey());
                        logger.debug("ClientServerConnection value is " + value);
                        logger.debug("ClientServerConnection latestMsg is " + latestMsg.getValue());
                        if (value == null && !latestMsg.getValue().equals("null")) {
                            Message msg = new Message(KVMessage.StatusType.CLIENT_SERVER_COMPROMISED, null, null, maybeCompromisedServer);
                            setNewServerSocket(thisServer);
                            sendMessage(msg);
                            closeConnection();
                        } else if (value != null && !value.equals(latestMsg.getValue())) {
                            Message msg = new Message(KVMessage.StatusType.CLIENT_SERVER_COMPROMISED, latestMsg.getKey(), value, maybeCompromisedServer);
                            setNewServerSocket(thisServer);
                            sendMessage(msg);
                            closeConnection();
                        }
                    } else if (latestMsg.getStatus() == KVMessage.StatusType.CLIENT_TRUE_GET) {
                        if (ClientListenRunnable.reports.containsKey(latestMsg.getKey())) {
                            System.out.println("The server you contacted has been compromised and has since been removed.");
                            System.out.println("The real value for " + latestMsg.getKey() + " is " + latestMsg.getValue());
                            System.out.println("Here at ZombieKV, we are deeply concerned about the safety of our customer's data.");
                            System.out.println("We are committed to work hard and not let a security incidence like this happen again.");
                            ClientListenRunnable.reports.remove(latestMsg.getKey());
                        } else {
                            ClientListenRunnable.reports.put(latestMsg.getKey(), latestMsg.getValue());
                        }
                        closeConnection();
                    } else if (latestMsg.getStatus() == KVMessage.StatusType.NOTIFICATION) {
                        System.out.println("Subscription update!");
                        System.out.println(latestMsg.getKey() + " has been updated to " + latestMsg.getValue());
                        closeConnection();
                    }
				/* connection either terminated by the client or lost due to
				 * network problems*/
                } catch (IOException ioe) {
                    logger.error("3Error! Connection lost due to input or output stream exception!", ioe);
                    isOpen = false;
                }
            }
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        } finally {
            closeConnection();
        }
    }

    private void setNewServerSocket(String serverKey) throws IOException {
        closeConnection();

        String[] splits = serverKey.split(":");
        String address = splits[0];
        Integer port = Integer.valueOf(splits[1]);
        socket = new Socket(address, port);
        input = socket.getInputStream();
        output = socket.getOutputStream();
    }

    public void closeConnection() {
        try {
            if (socket != null) {
                input.close();
                output.close();
                socket.close();
                isOpen = false;
                logger.info("Shutdown of connection with server <"
                        + socket.getInetAddress().getHostAddress() + ":"
                        + socket.getPort() + "> completed.");
                socket = null;
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down connection!", ioe);
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
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
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
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + message.getSerializedMsg() + "'");
        return message;
    }
}
