package app_admin;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class AdminClientConnection implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    private AdminCommInterface store;


    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public AdminClientConnection(Socket clientSocket, AdminCommInterface store) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.store = store;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while(isOpen) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    if (latestMsg == null){
                        logger.info("Connection lost due to input stream closed. Shutting down socket gracefully...");
                        isOpen = false;
                    }
                    else if (latestMsg.getStatus() == KVMessage.StatusType.ADMIN_SERVER_DEAD) {
                        String deadServerKey = latestMsg.getKey();
                        AdminFailureDetectionThread.mutexHandleFailureRecovery.lock();
                        try {
                            store.getNode(deadServerKey).getStore().pin();
                            logger.info("False alarm, Sever " + deadServerKey + " is not DEAD.");
                            isOpen = false;
                            AdminFailureDetectionThread.mutexHandleFailureRecovery.unlock();
                            break;
                        } catch (IOException e) {
                            try {
                                logger.info("Failure detected by a server that Server " + deadServerKey + " is DEAD.");
                                logger.info("Start the recovery process:");
                                store.removeNode(store.getNodeIndex(deadServerKey), true);
                                logger.debug("Successfully cleaned up DEAD Server: " + deadServerKey);
                                store.addNode(AdminClient.cacheSize, AdminClient.cacheStrategy);
                            } catch (Exception ex) {
                            }
                            isOpen = false;
                            AdminFailureDetectionThread.mutexHandleFailureRecovery.unlock();
                            break;
                        }
                    }
                    else if (latestMsg.getStatus() == KVMessage.StatusType.ADMIN_SERVER_COMPROMISED) {
                        String clientSocketIPandPort = clientSocket.getInetAddress().toString().replace("/","") + ":" + String.valueOf(clientSocket.getPort());
                        AdminFailureDetectionThread.mutexHandleFailureRecovery.lock();
                        if (AdminFailureDetectionThread.serverCompromisedCountMap.containsKey(latestMsg.getKey())){
                            String compromisedConnection = AdminFailureDetectionThread.serverCompromisedCountMap.get(latestMsg.getKey());
                            if (!compromisedConnection.equals("empty") && !compromisedConnection.equals(clientSocketIPandPort)) {
                                // this means two different servers have reported that this server is compromised, take actions now!
                                AdminFailureDetectionThread.serverCompromisedCountMap.put(latestMsg.getKey(), "empty");
                                // server has been compromised!
                                String compromisedServerKey = latestMsg.getKey();

                                try {
                                    logger.info("Detected by two servers that Server " + compromisedServerKey + " is COMPROMISED.");
                                    logger.info("Start the recovery process:");
                                    store.removeNode(store.getNodeIndex(compromisedServerKey), false);
                                    logger.debug("Successfully cleaned up COMPROMISED Server: " + compromisedServerKey);
                                    store.addNode(AdminClient.cacheSize, AdminClient.cacheStrategy);
                                } catch (Exception ex) {
                                }
                                isOpen = false;
                                AdminFailureDetectionThread.mutexHandleFailureRecovery.unlock();
                                break;
                            }
                            else if (compromisedConnection.equals("empty")){
                                // this means this is the first server that has reported that the particular server is compromised
                                AdminFailureDetectionThread.serverCompromisedCountMap.put(latestMsg.getKey(), clientSocketIPandPort);
                                AdminFailureDetectionThread.mutexHandleFailureRecovery.unlock();
                                isOpen = false;
                                break;
                            }
                            else if (compromisedConnection.equals(clientSocketIPandPort)) {
                                // this means the same server has reported another server multiple times, ignore the request!
                                isOpen = false;
                                break;
                            }
                        } else {
                            AdminFailureDetectionThread.serverCompromisedCountMap.put(latestMsg.getKey(), clientSocketIPandPort);
                            AdminFailureDetectionThread.mutexHandleFailureRecovery.unlock();
                            isOpen = false;
                            break;
                        }
                    }
                    else if (latestMsg.getStatus() == KVMessage.StatusType.SERVER_BROADCAST_UNSUBSCRIBE) {
                        String keyToUnsub = latestMsg.getKey();
                        String clientToUnsub = latestMsg.getValue();
                        try {
                            store.unsubscribeBroadcast(keyToUnsub, clientToUnsub);
                        } catch (Exception e){
                        }
                    }

				/* connection either terminated by the client or lost due to
				 * network problems*/
                } catch (IOException ioe) {
                    logger.info("Connection lost in receive message.");
                    isOpen = false;
                }
            }
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        } finally {
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                    logger.info("Shutdown of client socket <"
                            + clientSocket.getInetAddress().getHostAddress() + ":"
                            + clientSocket.getPort() + "> completed.");
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
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
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
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
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + message.getSerializedMsg() + "'");
        return message;
    }
}
