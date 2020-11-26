package app_server;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class HeartbeatRunnable implements Runnable {

    private Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket destinationSocket;
    private InputStream input;
    private OutputStream output;
    private Socket adminSocket;
    private boolean deadSocket;

    private String deadServerIP;
    private int deadServerPort;

    public HeartbeatRunnable(Socket destinationSocket, boolean deadSocket, String deadServerIP, int deadServerPort, String adminAddress) {
        this.destinationSocket = destinationSocket;
        this.isOpen = true;
        this.deadSocket = deadSocket;
        this.deadServerIP = deadServerIP;
        this.deadServerPort = deadServerPort;
        try {
            this.adminSocket = new Socket(adminAddress, 61034);
        } catch (IOException e) {
            logger.debug("Admin SOCKET CONNECTION FAILED!!" + e);
        }
        if (!deadSocket) {
            try {
                destinationSocket.setSoTimeout(5 * 1000); // timeout for 5 seconds
                // which means that read will block for 5 seconds
                // if the timeout expires, a java.net.SocketTimeoutException is raised.
                output = destinationSocket.getOutputStream();
                input = destinationSocket.getInputStream();
            } catch (IOException e) {
                logger.debug("heart beat connection/timeout failed to establish" + e);
            }
        }
    }

    public void close() throws IOException {
        this.isOpen = false;
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


    public void run() {
        if (!deadSocket) {
            while (isOpen) {
                try {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {

                    }
                    KVMessage heartBeat = null;
                    if (WelcomeThread.subscriptions.isEmpty()) {
                        heartBeat = new Message(KVMessage.StatusType.SERVER_HEART_BEAT, null, null, "null");
                    }
                    else {
                        heartBeat = new Message(KVMessage.StatusType.SERVER_HEART_BEAT, null, null, new Gson().toJson(WelcomeThread.subscriptions));
                    }
                    sendMessage(heartBeat);
                    try {
                        if (receiveMessage() == null) {
                            logger.debug("server: " + destinationSocket.getInetAddress().toString().replace("/", "") + ":" + destinationSocket.getPort() + " IS DEAD!");
                            // server is dead, time to inform admin
                            close();
                            output = adminSocket.getOutputStream();
                            input = adminSocket.getInputStream();
//                            String serverId = destinationSocket.getInetAddress().toString().replace("/", "") +
                            String serverId = destinationSocket.getInetAddress().getHostAddress() + ":" + destinationSocket.getPort();
                            KVMessage requestToAdmin = new Message(KVMessage.StatusType.ADMIN_SERVER_DEAD, serverId, null, null);
                            sendMessage(requestToAdmin);
                            closeAdminConnection();
                            break;
                        } else {
                            logger.debug("server: " + destinationSocket.getInetAddress().toString().replace("/", "") + ":" + destinationSocket.getPort() + " is alive!");
                        }
                    } catch (IOException e) {
                        logger.debug("server: " + destinationSocket.getInetAddress().toString().replace("/", "") + ":" + destinationSocket.getPort() + " IS DEAD!");
                        // server is dead, time to inform admin
                        close();
                        output = adminSocket.getOutputStream();
                        input = adminSocket.getInputStream();
                        String serverId = destinationSocket.getInetAddress().getHostAddress() + ":" + destinationSocket.getPort();
//                        String serverId = destinationSocket.getInetAddress().toString().replace("/", "") + ":" + destinationSocket.getPort();
                        KVMessage requestToAdmin = new Message(KVMessage.StatusType.ADMIN_SERVER_DEAD, serverId, null, null);
                        sendMessage(requestToAdmin);
                        closeAdminConnection();
                        break;
                    }
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {
                    }
                } catch (IOException e) {
                    logger.debug("Server to Server/Admin socket failed.");
                }
            }
        } else {
            try {
                output = adminSocket.getOutputStream();
                input = adminSocket.getInputStream();
                String serverId = deadServerIP + ":" + String.valueOf(deadServerPort);
                KVMessage requestToAdmin = new Message(KVMessage.StatusType.ADMIN_SERVER_DEAD, serverId, null, null);
                sendMessage(requestToAdmin);
                closeAdminConnection();
            } catch (IOException e) {
                logger.debug("Server to Admin socket failed. Server dead message unable to send. ");
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
