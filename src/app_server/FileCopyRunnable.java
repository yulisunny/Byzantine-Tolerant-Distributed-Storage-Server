package app_server;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.HashRange;
import common.Metadata;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;

public class FileCopyRunnable implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static volatile boolean isOpenToClientRequests = false;
    private static volatile boolean isServerInitialized = false;
    private static volatile boolean isWriteLocked = false;
    private static volatile Metadata metadata;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket destinationSocket;
    private InputStream input;
    private OutputStream output;
    private HashRange range;

    public FileCopyRunnable(Socket destinationSocket, HashRange range) {
        this.destinationSocket = destinationSocket;
        this.isOpen = true;
        this.range = range;
    }

    public void traverseDiskAndTransfer() throws Exception {
        String filename = WelcomeThread.getIPAddress()+String.valueOf(WelcomeThread.getPortNumber());
        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator + filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();

        String absoluteFilePath = absoluteDirPath + File.separator + "Data";
        File inputFile = new File(absoluteFilePath);

        if (!inputFile.exists()) {
            return;
        }

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for data file");
            throw e;
        }

        File tempFile = new File(absoluteDirPath + File.separator +"temp");
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
        } catch (IOException e) {
            logger.error("Cannot create writer for temporary file.");
            throw e;
        }

        try {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                CSVParser parser = CSVParser.parse(trimmedLine, CSVFormat.RFC4180);
                CSVRecord csvRecord = parser.getRecords().get(0);
                String key = csvRecord.get(0);
                String value = csvRecord.get(1);

                if (range.isInRange(WelcomeThread.getMd5Hash(key)))
                {
                    writer.write(currentLine + System.getProperty("line.separator"));
                    KVMessage replyMsg = new Message(KVMessage.StatusType.ADMIN_FILETRANSFER, key, value, null);
                    sendMessage(replyMsg);
                }
                else {
                    writer.write(currentLine + System.getProperty("line.separator"));
                }

              //  writer.write(currentLine + System.getProperty("line.separator"));
//                if (csvRecord.get(0).equals(key)) {
//                    value = csvRecord.get(1);
//                    break;
//                }
            }
            writer.close();
            reader.close();
        } catch (IOException e) {
            logger.error("Error reading date file or writing to temp file.");
            throw e;
        }

        if (!tempFile.renameTo(inputFile)) {
            logger.error("Error renaming");
            tempFile.delete();
        }
        return;
    }
    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = destinationSocket.getOutputStream();
            input = destinationSocket.getInputStream();
            traverseDiskAndTransfer();
            KVMessage replyMsg = new Message(KVMessage.StatusType.ADMIN_FILETRANSFER_COMPLETE, null, null, null);
            sendMessage(replyMsg);
            KVMessage message = receiveMessage();
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
