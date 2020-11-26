package app_server;

import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class NotificationRunnable implements Runnable {

    private Logger logger = Logger.getRootLogger();

    private String clieentServerSocketKey;
    private String key;
    private String value;

    public NotificationRunnable(String clientServerSocketKey, String key, String value) {
        this.clieentServerSocketKey = clientServerSocketKey;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        try {
            String[] splits = clieentServerSocketKey.split(":");
            String address = splits[0];
            Integer port = Integer.valueOf(splits[1]);
            Socket socket = new Socket(address, port);
            OutputStream output = socket.getOutputStream();
            Message msg = new Message(KVMessage.StatusType.NOTIFICATION, key, value, null);
            byte[] msgBytes = msg.getBytes();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
            logger.info("SEND \t<"
                    + socket.getInetAddress().getHostAddress() + ":"
                    + socket.getPort() + ">: '"
                    + msg.getSerializedMsg() +"'");
            output.close();
            socket.close();
        } catch (IOException e) {
            logger.error("Error in notify client", e);
        }
    }
}
