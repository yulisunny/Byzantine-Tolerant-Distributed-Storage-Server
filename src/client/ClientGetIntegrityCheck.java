package client;

import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import static common.messages.KVMessage.StatusType.CLIENT_GET_INTEGRITY_CHECK;

public class ClientGetIntegrityCheck extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private OutputStream output;

    private String verifyServer;
    private String suspiciousServer;
    private String key;
    private String value;

    public ClientGetIntegrityCheck(String suspiciousServer, String verifyServer, String key, String value) {
        this.verifyServer = verifyServer;
        this.suspiciousServer = suspiciousServer;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        String[] splits = verifyServer.split(":");
        String address = splits[0];
        Integer port = Integer.valueOf(splits[1]);

        try {
            socket = new Socket(address, port);
            output = socket.getOutputStream();
        } catch (IOException e) {
            logger.error("Cannot open Client Integrity check socket");
            return;
        }

        Message msg = new Message(CLIENT_GET_INTEGRITY_CHECK, key, value,
                suspiciousServer + "," + socket.getLocalAddress().getHostAddress() + ":" + ClientListenRunnable.getPortNumber());

        try {
            sendMessage(msg);
        } catch (IOException e) {
            logger.error("Error in sending Client get integrity check", e);
        }

        closeConnection();
    }

    public void closeConnection() {
        try {
            if (socket != null) {
                output.close();
                socket.close();
                socket = null;
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down connection!", ioe);
        }
    }

    private void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message:\t '" + msg.getSerializedMsg() + "'");
    }
}
