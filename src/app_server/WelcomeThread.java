package app_server;

import common.Metadata;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread that handles initialization of server side components such as cachedStorage, socket.
 */
public class WelcomeThread extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private static String ipAddr;
    private static int port;
    private static int serverCacheSize;
    private static String strategy;
    private static CachedStorage cachedStorage;
    private ServerSocket serverSocket;
    private boolean running;
    private static String successor = null;

    public static ReentrantLock isOpenToClientLock = new ReentrantLock(true);
    public static volatile boolean isOpenToClientRequests = false;

    //public static ReentrantLock writeLock = new ReentrantLock(true);
    public static volatile boolean isWriteLocked = false;
    public static volatile Metadata metadata;
    public static volatile boolean isServerInitialized = false;

    public static Socket destinationHeartbeatSocket;
    public static HeartbeatRunnable heartbeatRunnable;

    public static Map<String, List<String>> subscriptions = new HashMap<>();


    /**
     * Constructs a new welcome thread with given port.
     *
     * @param port      given port for storage server to operate
     */
    public WelcomeThread(String ipAddr, int port) {
        WelcomeThread.ipAddr = ipAddr;
        WelcomeThread.port = port;
    }

    /**
     * Starts the Server and listens for incoming connections.
     */
    @Override
    public void run() {
        running = bootupServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client, serverSocket, this);
                    new Thread(connection).start();
                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.debug("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }

    /**
     * Stops the server so that it won't listen at the given port any more.
     */
    public void stopServer() {
        running = false;
        try {
            serverSocket.close();
            heartbeatRunnable.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    /**
     * Checks whether the server is running.
     *
     * @return whether the server is running
     */
    private boolean isRunning() {
        return this.running;
    }

    private boolean bootupServer() {
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        } catch (IllegalArgumentException e) {
            logger.error("Error! Cannot open server socket: " + e.getMessage());
            return false;
        }
    }

    public static void startHeartbeatThread(String adminAddress) {
        logger.info("Starting Heartbeat Thread, admin is located at IP Address: " + adminAddress);
        String newSuccessor = metadata.getImmediateSuccessor(metadata.getHashRange(getIPAddress(), port).getEndHash()).getValue();
        // if the newSuccessor is of a different successor, start a new heartbeat with the new successor
        if (successor == null) {
            boolean isSocketDead = false;
            String deadServerIP = null;
            int deadServerPort = -1;
            successor = newSuccessor;
            String[] successorIpAndPort = successor.split(":");
            logger.debug("MY HEART BEAT SUCCESSOR IS: " + successor);
            try {
                destinationHeartbeatSocket = new Socket(successorIpAndPort[0], Integer.parseInt(successorIpAndPort[1]));
            } catch (IOException e) {
                isSocketDead = true;
                deadServerIP = successorIpAndPort[0];
                deadServerPort = Integer.parseInt(successorIpAndPort[1]);
                successor = null;
                logger.debug("Heart Beat Connection to Server " + successor + " failed. Report to admin that this server is dead.");
            }
            heartbeatRunnable = new HeartbeatRunnable(destinationHeartbeatSocket, isSocketDead, deadServerIP, deadServerPort, adminAddress);
            Thread heartbeatOperation = new Thread(heartbeatRunnable);
            heartbeatOperation.start();

        }
        else if (!successor.equals(newSuccessor)) {
            boolean isSocketDead = false;
            String deadServerIP = null;
            int deadServerPort = -1;
            try {
                heartbeatRunnable.close();
            } catch (IOException e) {
                logger.debug("Failed to close connection to previous heart beat, possible reason: on the other end server dead");
            }
            successor = newSuccessor;
            String[] successorIpAndPort = successor.split(":");
            logger.debug("MY HEART BEAT SUCCESSOR IS: " + successor);
            try {
                destinationHeartbeatSocket = new Socket(successorIpAndPort[0], Integer.parseInt(successorIpAndPort[1]));
            } catch (IOException e) {
                isSocketDead = true;
                deadServerIP = successorIpAndPort[0];
                deadServerPort = Integer.parseInt(successorIpAndPort[1]);
                successor = null;
                logger.debug("Heart Beat Connection to Server " + successor + " failed. Report to admin that this server is dead.");
            }
            heartbeatRunnable = new HeartbeatRunnable(destinationHeartbeatSocket, isSocketDead, deadServerIP, deadServerPort, adminAddress);
            Thread heartbeatOperation = new Thread(heartbeatRunnable);
            heartbeatOperation.start();
        }
        // else, it means that the successor is still the same, no start of new heart beat
    }

    /**
     * Obtains the current server side cachedStorage for key-value pairs.
     *
     * @return cachedStorage instance on server side
     */
    public static CachedStorage getCachedStorage() {
        return cachedStorage;
    }


    public static boolean initializeServer (int cacheSize, String strategy, String serializedMetadata) {
        serverCacheSize = cacheSize;
        switch (strategy) {
            case "FIFO":
                cachedStorage = new FifoCachedStorage(cacheSize, getIPAddress(), getPortNumber());
                break;
            case "LRU":
                cachedStorage = new LruCachedStorage(cacheSize, getIPAddress(), getPortNumber());
                break;
            case "LFU":
                cachedStorage = new LfuCachedStorage(cacheSize, getIPAddress(), getPortNumber());
                break;
            case "FIFO_UNIQUE":
                cachedStorage = new FifoUniqueKeyCachedStorage(cacheSize, getIPAddress(), getPortNumber());
                break;
            default:
                logger.error(strategy + " is not a valid caching strategy.");
                System.out.println("Error! Valid caching strategies are FIFO, LRU and LFU.");
                return false;
        }
        metadata = Metadata.deserialize(serializedMetadata);
        isServerInitialized = true;
        return true;
    }

    public static int getPortNumber() { return port; }

    public static int getServerCacheSize() { return serverCacheSize; }

    public static String getIPAddress() { return ipAddr; }

    public static String getMd5Hash(String input) {
        byte[] bytesOfMessage = input.getBytes();
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.error("The MD5 message digest provider was not found.");
            return "";
        }

        String hex = (new HexBinaryAdapter()).marshal(md.digest(bytesOfMessage));
        return hex;
    }

    public static String writeToDisk(String key, String value) throws IOException {
        String filename = getIPAddress()+String.valueOf(getPortNumber());

        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator + filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();

        String absoluteFilePath = absoluteDirPath + File.separator + "Data";
        File inputFile = new File(absoluteFilePath);
        try {
            inputFile.createNewFile();
        } catch (IOException e) {
            logger.error("Cannot create or read data file.");
            throw e;
        }

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for data file");
            throw e;
        }

        File tempFile = new File(absoluteDirPath + File.pathSeparator + "temp");
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
        } catch (IOException e) {
            logger.error("Cannot create writer for temporary file.");
            throw e;
        }

        String rtn = null;
        try {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                CSVParser parser = CSVParser.parse(trimmedLine, CSVFormat.RFC4180);
                CSVRecord csvRecord = parser.getRecords().get(0);
                if (csvRecord.get(0).equals(key)) {
                    rtn = csvRecord.get(1);
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            String escapedKey = StringEscapeUtils.escapeCsv(key);
            String escapedValue = StringEscapeUtils.escapeCsv(value);
            String newLine = escapedKey + "," + escapedValue;
            writer.write(newLine + System.getProperty("line.separator"));
            writer.close();
            reader.close();
        } catch (IOException e) {
            logger.error("Error while copying data to temporary file.");
            tempFile.delete();
            throw e;
        } catch (Exception e) {

        }

        if (!tempFile.renameTo(inputFile)) {
            tempFile.delete();
            rtn = null;
        }
        return rtn;
    }

    public static String deleteFromDisk(String key) throws IOException {
        String filename = getIPAddress()+String.valueOf(getPortNumber());

        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator +filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();

        String absoluteFilePath = absoluteDirPath + File.separator + "Data";

        File inputFile = new File(absoluteFilePath);
        if (!inputFile.exists()) {
            return null;
        }

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for data file");
            throw e;
        }

        File tempFile = new File(absoluteDirPath + File.separator + "temp");
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
        } catch (IOException e) {
            logger.error("Cannot create writer for temporary file.");
            throw e;
        }

        String rtn = null;
        try {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                CSVParser parser = CSVParser.parse(trimmedLine, CSVFormat.RFC4180);
                CSVRecord csvRecord = parser.getRecords().get(0);
                if (csvRecord.get(0).equals(key)) {
                    rtn = csvRecord.get(1);
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
        } catch (IOException e) {
            logger.error("Error while copying data to temporary file.");
            tempFile.delete();
            throw e;
        }

        if (!tempFile.renameTo(inputFile)) {
            tempFile.delete();
            rtn = null;
        }
        return rtn;
    }

    public static void stopClientRequests() throws Exception {
        isOpenToClientRequests = false;
        // Since it's a fair lock and we blocked new threads from coming in, then once we get the lock, it will be the last one in the line
        // and that means that once we acquire the lock, no client connection is waiting.
        isOpenToClientLock.lock();
        isOpenToClientLock.unlock();
    }

    public static void startClientRequests() throws Exception {
        isOpenToClientRequests = true;
    }

    public static void lockWrite() {
        isWriteLocked = true;
        isOpenToClientLock.lock();
        isOpenToClientLock.unlock();
    }

    public static void unlockWrite() {
        isWriteLocked = false;
    }

    public static void updateMetadata(String pMetadata) {
        metadata = Metadata.deserialize(pMetadata);
    }
}
