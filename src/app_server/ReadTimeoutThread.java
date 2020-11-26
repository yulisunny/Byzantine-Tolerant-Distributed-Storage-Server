package app_server;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ReadTimeoutThread extends Thread {
    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private ServerSocket listeningSocket;
    private InputStream input;
    private OutputStream output;

    public ReadTimeoutThread(Socket socket) {
        this.socket = socket;
        try {
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
        } catch (IOException e) {

        }
    }

    @Override
    public void run() {
        try {
            logger.debug("RECEIVE MESSAGE INFO");
            KVMessage message = receiveMessage();
            if (message == null) {

            }
            logger.debug("MEssage is :" + message);
            logger.debug("RECEIVE MESSAGE INFO FINISHED");
        } catch (IOException e) {
            logger.debug("EHFEFEFEFEF");
        }
    }

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
//        logger.debug("RECEIVE MESSAGE HAHAHA");
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
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
            read = (byte) input.read();
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

//        logger.info("RECEIVE \t<"
//                + clientSocket.getInetAddress().getHostAddress() + ":"
//                + clientSocket.getPort() + ">: '"
//                + message.getSerializedMsg() + "'");
//        logger.debug("RECEIVE MESSAGE deadaefffff");

        return message;
    }

}
