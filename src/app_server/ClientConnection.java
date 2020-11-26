package app_server;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import common.HashRange;
import common.Metadata;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClientConnection implements Runnable {

    private Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private ServerSocket listeningSocket;
    private InputStream input;
    private OutputStream output;
    private WelcomeThread mainThread;
    private int getCompromisedServerPortNumber = 60008;
    private int putCompromisedServerPortNumber = 60009;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, ServerSocket listeningSocket, WelcomeThread mainThread) {
        this.clientSocket = clientSocket;
        this.listeningSocket = listeningSocket;
        this.mainThread = mainThread;
        this.isOpen = true;
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
                    else if (latestMsg.getStatus() == KVMessage.StatusType.PUT || latestMsg.getStatus() == KVMessage.StatusType.GET) {
                        KVMessage replyMsg;
                        if (WelcomeThread.isOpenToClientRequests) { //isOpenToClientRequests
                            if (!WelcomeThread.metadata.hasServer(WelcomeThread.getIPAddress(), WelcomeThread.getPortNumber())) {
                                replyMsg = new Message(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null, WelcomeThread.metadata.getSerializedForm());
                                sendMessage(replyMsg);
                                continue;
                            }
                            switch (latestMsg.getStatus()) {
                                case PUT:
                                    if (!WelcomeThread.isWriteLocked &&
                                            WelcomeThread.metadata.getHashRange(WelcomeThread.getIPAddress(), WelcomeThread.getPortNumber()).isInRange(WelcomeThread.getMd5Hash(latestMsg.getKey())))
                                    {
                                        WelcomeThread.isOpenToClientLock.lock();
                                        if (latestMsg.getValue().equals("null") || latestMsg.getValue().equals("")) {
                                            replyMsg = WelcomeThread.getCachedStorage().delete(latestMsg.getKey());
                                        } else {
                                            replyMsg = WelcomeThread.getCachedStorage().put(latestMsg.getKey(), latestMsg.getValue());
                                        }
                                        sendMessage(replyMsg);
                                        WelcomeThread.isOpenToClientLock.unlock();



                                        // M4: Replication of write operations to its 2 replicas with serverKey,clientKey
                                        List<String> replicas = WelcomeThread.metadata.getBackups(WelcomeThread.getMd5Hash(latestMsg.getKey()));
                                        String serverKey = WelcomeThread.getIPAddress() + ":" + String.valueOf(WelcomeThread.getPortNumber());
                                        String key = latestMsg.getKey();
                                        String value = latestMsg.getValue();
                                        if (WelcomeThread.getPortNumber() == putCompromisedServerPortNumber) {
                                            // M4: This is a compromised server, will change the key,value pair in the replication process to change data
                                            // on the two Replicas
                                            value = "COMPROMISED!";
                                        }

                                        String[] replicaServerAndPort1 = replicas.get(0).split(":");
                                        Socket destinationReplica1Socket = new Socket(replicaServerAndPort1[0], Integer.parseInt(replicaServerAndPort1[1]));
                                        WriteReplicationRunnable writeReplicationRunnable1 = new WriteReplicationRunnable(destinationReplica1Socket, key, value, serverKey, latestMsg.getMetadata());
                                        new Thread(writeReplicationRunnable1).start();

                                        String[] replicaServerAndPort2 = replicas.get(1).split(":");
                                        Socket destinationReplica2Socket = new Socket(replicaServerAndPort2[0], Integer.parseInt(replicaServerAndPort2[1]));
                                        WriteReplicationRunnable writeReplicationRunnable2 = new WriteReplicationRunnable(destinationReplica2Socket, key, value, serverKey, latestMsg.getMetadata());
                                        new Thread(writeReplicationRunnable2).start();

                                        // M4: Notify subscription
                                        List<String> clientServerSocketKey = WelcomeThread.subscriptions.get(replyMsg.getKey());
                                        if (clientServerSocketKey != null) {
                                            for (String c : clientServerSocketKey) {
                                                NotificationRunnable notify = new NotificationRunnable(c, replyMsg.getKey(), replyMsg.getValue());
                                                new Thread(notify).start();
                                                logger.debug("Sending client socket " + c + " about " + replyMsg.getKey() + replyMsg.getValue());
                                            }
                                        }

                                    }
                                    else if (WelcomeThread.isWriteLocked) {
                                        replyMsg = new Message(KVMessage.StatusType.SERVER_WRITE_LOCK, null, null, null);
                                        sendMessage(replyMsg);
                                    }
                                    else {
                                        replyMsg = new Message(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null, WelcomeThread.metadata.getSerializedForm());
                                        sendMessage(replyMsg);
                                    }
                                    break;
                                case GET:
                                    if (WelcomeThread.metadata.getHashRange(WelcomeThread.getIPAddress(), WelcomeThread.getPortNumber()).isInReadRange(WelcomeThread.getMd5Hash(latestMsg.getKey()))) {
                                        // GET doesn't need this lock really.
                                        //WelcomeThread.isOpenToClientLock.lock();
                                        if (WelcomeThread.getPortNumber() == getCompromisedServerPortNumber) {
                                            // M4: This is a compromised server, it is going to return Compromised no matter what the actual value is
                                            replyMsg = new Message(KVMessage.StatusType.GET_SUCCESS, latestMsg.getKey(), "COMPROMISED!", null);
                                            sendMessage(replyMsg);
                                        }
                                        else {
                                            // M4: Normal (Not compromised) Server
                                            replyMsg = WelcomeThread.getCachedStorage().get(latestMsg.getKey());
                                            sendMessage(replyMsg);
                                        }
                                        //WelcomeThread.isOpenToClientLock.unlock();
                                    } else {
                                        replyMsg = new Message(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null, WelcomeThread.metadata.getSerializedForm());
                                        sendMessage(replyMsg);
                                    }
                                    break;
                                default:
                                    replyMsg = new Message(KVMessage.StatusType.FAILED, null, null, null);
                                    sendMessage(replyMsg);
                                    break;
                            }
                        }
                        else {
                            replyMsg = new Message(KVMessage.StatusType.SERVER_STOPPED, null, null, WelcomeThread.metadata.getSerializedForm());
                            sendMessage(replyMsg);
                        }
                    }
                    else {
                        KVMessage replyMsg;
                        switch (latestMsg.getStatus()) {
                            case ADMIN_INIT_KVSERVER:
                                WelcomeThread.initializeServer(Integer.parseInt(latestMsg.getKey()), latestMsg.getValue(), latestMsg.getMetadata());
                                WelcomeThread.startHeartbeatThread(clientSocket.getInetAddress().getHostAddress());
                                //WelcomeThread.isServerInitialized = true;
                                break;
                            case ADMIN_START:
                                if (WelcomeThread.isServerInitialized) {
                                    try {
                                        WelcomeThread.startClientRequests();
                                    } catch (Exception e) {

                                    }
                                    //WelcomeThread.isOpenToClientRequests = true;
                                }
                                else {
                                    replyMsg = new Message(KVMessage.StatusType.FAILED, null, null, null);
                                    sendMessage(replyMsg);
                                }
                                break;
                            case ADMIN_STOP:
                                try {
                                    WelcomeThread.stopClientRequests();
                                } catch (Exception e) {

                                }
                                //WelcomeThread.isOpenToClientRequests = false;
                                break;
                            case ADMIN_SHUTDOWN:
//                                try {
                                logger.info("Closing server listening socket " + listeningSocket.getLocalPort() + "...");
                                listeningSocket.getLocalPort();
                                mainThread.stopServer();
                                logger.info("Server listening socket closed.");
//                                } catch (IOException e) {
//                                    logger.error("Error! " +
//                                            "Unable to close server listening socket", e);
//                                }
                                System.exit(0);
                                break;
                            case ADMIN_LOCKWRITE:
                                //WelcomeThread.isWriteLocked = true;
                                WelcomeThread.lockWrite();
                                break;
                            case ADMIN_UNLOCKWRITE:
                                WelcomeThread.unlockWrite();
                                //WelcomeThread.isWriteLocked = false;
                                break;
                            case ADMIN_MOVEDATA:
                                try {
                                    Socket destinationSocket = new Socket(latestMsg.getKey(), Integer.parseInt(latestMsg.getValue()));
                                    HashRange range = Metadata.deserialize(latestMsg.getMetadata()).getHashRange(latestMsg.getKey(), Integer.parseInt(latestMsg.getValue()));
                                    FileTransferRunnable fileTransferRunnable = new FileTransferRunnable(destinationSocket, range);
                                    Thread fileTransfer = new Thread(fileTransferRunnable);
                                    fileTransfer.start();
                                    while (fileTransfer.getState()!=Thread.State.TERMINATED) {}
                                    replyMsg = new Message(KVMessage.StatusType.ADMIN_MOVEDATA, null, null, null);
                                    sendMessage(replyMsg);
                                } catch (IOException e) {
                                    replyMsg = new Message(KVMessage.StatusType.FAILED, null, null, null);
                                    sendMessage(replyMsg);
                                    logger.debug("MOVEDATA FAILED, Destination server socket not alive.");
                                }
                                break;
                            case ADMIN_COPYDATA:
                                try {
                                    Socket destinationSocket = new Socket(latestMsg.getKey(), Integer.parseInt(latestMsg.getValue()));
                                    HashRange range = Metadata.deserialize(latestMsg.getMetadata()).getHashRange(latestMsg.getKey(), Integer.parseInt(latestMsg.getValue()));
                                    FileCopyRunnable fileCopyRunnable = new FileCopyRunnable(destinationSocket, range);
                                    Thread newFileTransfer = new Thread(fileCopyRunnable);
                                    newFileTransfer.start();
                                    while (newFileTransfer.getState() != Thread.State.TERMINATED) {
                                    }
                                    replyMsg = new Message(KVMessage.StatusType.ADMIN_COPYDATA, null, null, null);
                                    sendMessage(replyMsg);
                                } catch (IOException e) {
                                    replyMsg = new Message(KVMessage.StatusType.FAILED, null, null, null);
                                    sendMessage(replyMsg);
                                    logger.debug("COPYDATA FAILED, Destination server socket not alive.");
                                }
                                break;
                            case ADMIN_UPDATE:
                                WelcomeThread.updateMetadata(latestMsg.getMetadata());
                                WelcomeThread.startHeartbeatThread(clientSocket.getInetAddress().getHostAddress());
                                //WelcomeThread.metadata = Metadata.deserialize(latestMsg.getMetadata());
                                break;
                            case ADMIN_FILETRANSFER:
//                                if (latestMsg.getValue().equals("null") || latestMsg.getValue().equals("")) {
//                                    WelcomeThread.deleteFromDisk(latestMsg.getKey());
//                                }
//                                else {
//                                WelcomeThread.writeToDisk(latestMsg.getKey(), latestMsg.getValue());
                                WelcomeThread.getCachedStorage().put(latestMsg.getKey(), latestMsg.getValue());
//                                }
                                break;
                            case ADMIN_FILETRANSFER_COMPLETE:
                                // send back the message got from the other server as an ack to indicate that file transfer is complete
                                sendMessage(latestMsg);
                                break;
                            case SERVER_HEART_BEAT:
                                if (!latestMsg.getMetadata().equals("null")) {
                                    Type type = new TypeToken<Map<String, List<String>>>(){}.getType();
                                    Map<String, List<String>> subscriptions = new Gson().fromJson(latestMsg.getMetadata(), type);
//                                    WelcomeThread.subscriptions.putAll(subscriptions);
                                    for (String key : subscriptions.keySet()) {
                                        if (!WelcomeThread.subscriptions.containsKey(key)) {
                                            WelcomeThread.subscriptions.put(key, subscriptions.get(key));
                                        }
                                        else {
                                            for (String clientKey : subscriptions.get(key)) {
                                                if (!WelcomeThread.subscriptions.get(key).contains(clientKey)) {
                                                    WelcomeThread.subscriptions.get(key).add(clientKey);
                                                }
                                            }
                                        }
                                    }
                                }
                                replyMsg = new Message(KVMessage.StatusType.SERVER_HEART_BEAT_REPLY, null, null, null);
                                sendMessage(replyMsg);
                                break;
                            case ADMIN_DELETEDATA:
                                String filename = WelcomeThread.getIPAddress()+String.valueOf(WelcomeThread.getPortNumber());
                                String workingDirectory = System.getProperty("user.dir");
                                String absoluteDirPath = workingDirectory + File.separator + filename;
                                File dir = new File(absoluteDirPath);
                                String absoluteFilePath = absoluteDirPath + File.separator + "Data";
                                File inputFile = new File(absoluteFilePath);
                                if (!inputFile.exists()) {
                                    sendMessage(latestMsg);
                                    break;
                                }
                                PrintWriter writer = new PrintWriter(inputFile);
                                writer.print("");
                                writer.close();
                                sendMessage(latestMsg);
                                break;
                            case ADMIN_PIN:
                                sendMessage(latestMsg);
                                break;
                            case ADMIN_REPLICATION:
                                if (latestMsg.getValue().equals("null") || latestMsg.getValue().equals("")) {
                                    // this case needed for handling replication deletes
//                                    WelcomeThread.deleteFromDisk(latestMsg.getKey());
                                    WelcomeThread.getCachedStorage().delete(latestMsg.getKey());
                                }
                                else {
                                    // this case for handling move date transfer and replication writes
//                                    WelcomeThread.writeToDisk(latestMsg.getKey(), latestMsg.getValue());
                                    WelcomeThread.getCachedStorage().put(latestMsg.getKey(), latestMsg.getValue());
                                }

                                String server_client_key = latestMsg.getMetadata();
                                String [] server_client_key_array = server_client_key.split(",");
                                String serverKey = server_client_key_array[0];
                                String clientKey = server_client_key_array[1];
                                String [] client_ip_port_array = clientKey.split(":");
                                String client_ip_address = client_ip_port_array[0];
                                String client_port = client_ip_port_array[1];

                                String ownIPandPort = WelcomeThread.getIPAddress() + ":" + WelcomeThread.getPortNumber();

                                if (!client_port.equals("1")) {
                                    try {
                                        Socket destinationClientSocket = new Socket(client_ip_address, Integer.parseInt(client_port));
                                        ClientIntegrityCheckRunnable clientIntegrityCheckRunnable =
                                                new ClientIntegrityCheckRunnable(
                                                        destinationClientSocket,
                                                        latestMsg.getKey(),
                                                        latestMsg.getValue(),
                                                        serverKey,
                                                        false,
                                                        KVMessage.StatusType.PUT,
                                                        "138.68.229.113",
                                                        ownIPandPort);
                                        new Thread(clientIntegrityCheckRunnable).start();
                                    } catch (Exception e) {
                                        ClientIntegrityCheckRunnable clientIntegrityCheckRunnable =
                                                new ClientIntegrityCheckRunnable(
                                                        null,
                                                        null,
                                                        null,
                                                        serverKey,
                                                        true,
                                                        null,
                                                        "138.68.229.113",
                                                        ownIPandPort);
                                        new Thread(clientIntegrityCheckRunnable).start();
                                        // Remove the key from compromised server
//                                        WelcomeThread.deleteFromDisk(latestMsg.getKey());
                                        WelcomeThread.getCachedStorage().delete(latestMsg.getKey());
                                    }
                                }
                                break;
                            case CLIENT_SERVER_COMPROMISED:
                                String compromisedServerKey = latestMsg.getMetadata();
                                ownIPandPort = WelcomeThread.getIPAddress() + ":" + WelcomeThread.getPortNumber();
                                ClientIntegrityCheckRunnable clientIntegrityCheckRunnable =
                                        new ClientIntegrityCheckRunnable(
                                                null,
                                                null,
                                                null,
                                                compromisedServerKey,
                                                true,
                                                null,
                                                "138.68.229.113",
                                                ownIPandPort);
                                new Thread(clientIntegrityCheckRunnable).start();
                                // Redo the client request.
                                if (latestMsg.getValue().equals("null") || latestMsg.getValue().equals("")) {
                                    // Delete op
//                                    WelcomeThread.deleteFromDisk(latestMsg.getKey());
                                    WelcomeThread.getCachedStorage().delete(latestMsg.getKey());
                                }
                                else {
                                    // Write op
//                                    WelcomeThread.writeToDisk(latestMsg.getKey(), latestMsg.getValue());
                                    WelcomeThread.getCachedStorage().put(latestMsg.getKey(), latestMsg.getValue());
                                }
                                break;
                            case CLIENT_GET_INTEGRITY_CHECK:
                                server_client_key = latestMsg.getMetadata();
                                server_client_key_array = server_client_key.split(",");
                                serverKey = server_client_key_array[0];
                                clientKey = server_client_key_array[1];
                                client_ip_port_array = clientKey.split(":");
                                client_ip_address = client_ip_port_array[0];
                                client_port = client_ip_port_array[1];

                                ownIPandPort = WelcomeThread.getIPAddress() + ":" + WelcomeThread.getPortNumber();

                                KVMessage value = WelcomeThread.getCachedStorage().get(latestMsg.getKey());
                                logger.debug("value is " + value.getSerializedMsg());
                                logger.debug("latestMsg is " + latestMsg.getSerializedMsg());
                                if (value.getValue() == null && (latestMsg.getValue() == null || latestMsg.getValue().equals("null"))) {
                                    break;
                                }

                                if (value != null && value.getStatus()== KVMessage.StatusType.GET_SUCCESS) {
                                    if (!value.getValue().equals(latestMsg.getValue())) {

                                        // The get failed, no value for this key, integrity check failed
                                        // Inform Admin to recovery the compromised server
                                        ClientIntegrityCheckRunnable informAdminServerCompromisedRunnable =
                                                new ClientIntegrityCheckRunnable(
                                                        null,
                                                        null,
                                                        null,
                                                        serverKey,
                                                        true,
                                                        null,
                                                        "138.68.229.113" ,
                                                        ownIPandPort);
                                        new Thread(informAdminServerCompromisedRunnable).start();
                                        // Inform Client that the read value was wrong

                                        Socket destinationClientSocket = new Socket(client_ip_address, Integer.parseInt(client_port));
                                        ClientIntegrityCheckRunnable clientGetIntegrityCheckRunnable =
                                                new ClientIntegrityCheckRunnable(
                                                        destinationClientSocket,
                                                        latestMsg.getKey(),
                                                        value.getValue(),
                                                        serverKey,
                                                        false,
                                                        KVMessage.StatusType.GET,
                                                        "138.68.229.113" ,
                                                        ownIPandPort);
                                        new Thread(clientGetIntegrityCheckRunnable).start();
                                    }
                                }
                                else {
                                    // The get failed, no value for this key, integrity check failed
                                    // Inform Admin to recovery the compromised server
                                    ClientIntegrityCheckRunnable informAdminServerCompromisedRunnable =
                                            new ClientIntegrityCheckRunnable(
                                                    null,
                                                    null,
                                                    null,
                                                    serverKey,
                                                    true,
                                                    null,
                                                    "138.68.229.113",
                                                    ownIPandPort);
                                    new Thread(informAdminServerCompromisedRunnable).start();
                                    // Inform Client that the read value was wrong

                                    Socket destinationClientSocket = new Socket(client_ip_address, Integer.parseInt(client_port));
                                    ClientIntegrityCheckRunnable clientGetIntegrityCheckRunnable =
                                            new ClientIntegrityCheckRunnable(
                                                    destinationClientSocket,
                                                    latestMsg.getKey(),
                                                    value.getValue(),
                                                    serverKey,
                                                    false,
                                                    KVMessage.StatusType.GET,
                                                    "138.68.229.113",
                                                    ownIPandPort);
                                    new Thread(clientGetIntegrityCheckRunnable).start();
                                }
                                break;
                            case SUBSCRIBE:
                                if (WelcomeThread.metadata.getHashRange(WelcomeThread.getIPAddress(), WelcomeThread.getPortNumber()).isInReadRange(WelcomeThread.getMd5Hash(latestMsg.getKey()))) {
                                    List<String> notifyThese = WelcomeThread.subscriptions.get(latestMsg.getKey());
                                    if (notifyThese == null) {
                                        List<String> clients = new ArrayList<>();
                                        clients.add(latestMsg.getMetadata());
                                        WelcomeThread.subscriptions.put(latestMsg.getKey(), clients);
                                    } else {
                                        notifyThese.add(latestMsg.getMetadata());
                                        WelcomeThread.subscriptions.put(latestMsg.getKey(), notifyThese);
                                    }
                                    replyMsg = new Message(KVMessage.StatusType.SUBSCRIBE_SUCCESS, null, null, null);
                                    sendMessage(replyMsg);
                                }
                                else {
                                    replyMsg = new Message(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null, WelcomeThread.metadata.getSerializedForm());
                                    sendMessage(replyMsg);
                                }
                                break;
                            case UNSUBSCRIBE:
                                ClientIntegrityCheckRunnable adminUnsubscribeRequestRunnable =
                                        new ClientIntegrityCheckRunnable(null,
                                                latestMsg.getKey(),
                                                latestMsg.getMetadata(),
                                                null,
                                                false,
                                                KVMessage.StatusType.UNSUBSCRIBE,
                                                "138.68.229.113",
                                                null);
                                new Thread(adminUnsubscribeRequestRunnable).start();
//                                List<String> these = WelcomeThread.subscriptions.get(latestMsg.getKey());
//                                if (these != null) {
//                                    these.remove(latestMsg.getMetadata());
//                                    if (these.size() == 0) {
//                                        WelcomeThread.subscriptions.remove(latestMsg.getKey());
//                                    }
//                                }
                                break;
                            case ADMIN_UNSUBSCRIBE:
                                String keyToUnsub = latestMsg.getKey();
                                String clientToUnsub = latestMsg.getValue();
                                if (WelcomeThread.subscriptions.containsKey(keyToUnsub) && WelcomeThread.subscriptions.get(keyToUnsub).contains(clientToUnsub)) {
                                    WelcomeThread.subscriptions.get(keyToUnsub).remove(clientToUnsub);
//                                    if (WelcomeThread.subscriptions.get(keyToUnsub).size() == 0) {
//                                        WelcomeThread.subscriptions.remove(keyToUnsub);
//                                    }
                                }
                                break;
                            default:
                                replyMsg = new Message(KVMessage.StatusType.FAILED, null, null, null);
                                sendMessage(replyMsg);
                                break;
                        }
                    }

				/* connection either terminated by the client or lost due to
				 * network problems*/
                } catch (IOException ioe) {
                    logger.info("1Error! Connection lost due to input or output stream exception!", ioe);
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
        logger.info("SEND <"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '" + msg.getStatus() + "'");
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

        logger.info("RECEIVE <"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + message.getStatus() + "'");
        return message;
    }
}