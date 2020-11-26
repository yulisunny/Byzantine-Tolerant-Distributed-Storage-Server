package app_client;

import client.ClientListenRunnable;
import client.CommInterface;
import client.Store;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

/**
 * Application APIs that supports client side command line shell interactions with the
 * storage server.
 *
 * @see Store
 * @see common.messages.KVMessage
 */
public class Client {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "Client> ";

    private BufferedReader stdin;
    private CommInterface store;
    private Boolean stop = false;

    // Server
    private String serverAddress;
    private int serverPort;

    private ClientListenRunnable listeningRunnable;

    /**
     * Initiates the command line shell and handle client requests.
     */
    public void run() {
        listeningRunnable = new ClientListenRunnable();
        new Thread(listeningRunnable, "client-listen").start();
        logger.info("Started a listening Thread.");
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                logger.debug(String.format("Received input from client: %s", cmdLine));
                handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                logger.error(String.format("CLI received I/O error. %s.", e.getMessage()));
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    /**
     * Parses and initiate calls to handlers for each client request.
     *
     * @param cmdLine client request string
     */
    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("quit")) {
            handleQuit();
        } else if (tokens[0].equals("connect")) {
            handleConnect(tokens);
        } else if (tokens[0].equals("disconnect")) {
            disconnect();
        } else if (tokens[0].equals("get")) {
            handleGet(tokens);
        } else if (tokens[0].equals("put")) {
            handlePut(tokens);
        } else if (tokens[0].equals("logLevel")) {
            handleLogLevel(tokens);
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else if (tokens[0].equals("subscribe")) {
            handleSubscribe(tokens);
        } else if (tokens[0].equals("unsubscribe")) {
            handleUnsubscribe(tokens);
        } else {
            printError("Unknown command.");
            printHelp();
        }
    }

    private void handleSubscribe(String[] tokens) {
        if (tokens.length == 2) {
            subscribe(tokens[1]);
        } else {
            printError("Usage: subscribe key");
        }
    }

    private void subscribe(String key) {
        if (store == null) {
            printError("Cannot perform subscription. Not connected to any server.");
            return;
        }

        if (!store.subscribe(key)) {
            printError("Subscription failed.");
        }
    }

    private void handleUnsubscribe(String[] tokens) {
        if (tokens.length == 2) {
            unsubscribe(tokens[1]);
        } else {
            printError("Usage: unsubscribe key");
        }
    }

    private void unsubscribe(String key) {
        if (store == null) {
            printError("Cannot perform unsubscription. Not connected to any server.");
            return;
        }

        if (!store.unsubscribe(key)) {
            printError("Subscription failed.");
        }
    }

    /**
     * Handles client GET request to the storage server.
     *
     * @param tokens array of GET request parameters
     */
    private void handleGet(String[] tokens) {
        if (tokens.length >= 2) {
            get(parseGetParams(tokens));
        } else {
            printError("Invalid number of parameters.");
        }
    }

    /**
     * Parses GET keys that has whitespace into one space delimited string.
     *
     * @param tokens tokens of spaced delimited GET request parameters
     * @return space delimited string as key for GET request
     */
    private String parseGetParams(String[] tokens) {
        StringBuilder msg = new StringBuilder();
        for (int i = 1; i < tokens.length; i++) {
            msg.append(tokens[i]);
            if (i != tokens.length - 1) {
                msg.append(" ");
            }
        }
        return msg.toString();
    }

    /**
     * Performs the GET request using API from Store.
     *
     * @param key GET request key
     */
    private void get(String key) {
        if (store == null) {
            printError("Cannot perform get. Not connected to any server.");
            return;
        }

        KVMessage message;
        logger.debug(String.format("Performing GET with key %s", key));

        // Perform Get
        try {
            message = store.get(key);
        } catch (IOException e) {
            logger.debug(String.format("Failed GET with key %s. I/O Exception: %s", key, e.getMessage()));
            printError(String.format("Received I/O error. %s.", e.getMessage()));
            resetConnection();
            return;
        } catch (IllegalStateException e) {
            resetConnection();
            logger.debug(String.format("Failed GET with key %s. %s.", key, e.getMessage()));
            printError(e.getMessage());
            return;
        } catch (Exception e) {
            logger.debug(String.format("Failed GET with key %s", key), e);
            printError(String.format("Encountered exception while performing GET with key %s. %s.",
                    key, e.getMessage()));
            return;
        }

        // Parse response
        logger.debug(String.format("Received GET reply from server: %s", message.toString()));
        StatusType status = message.getStatus();
        if (status.equals(StatusType.GET_SUCCESS)) {
            printToPrompt(message.getValue());

        } else if (status.equals(StatusType.GET_ERROR)) {
            printError(String.format("Storage server does not have a value for key %s.", key));
        } else if (status.equals(StatusType.SERVER_STOPPED)) {
            printError(String.format("Server is not open to client requests"));
        } else {
            logger.error(String.format("GET reply with invalid status code %s", message.toString()));
            printError(String.format("Failed to perform GET with key %s. Expected GET response, received %s.",
                    key, message.toString()));
        }
    }

    /**
     * Handles client request to connect to storage server.
     *
     * @param tokens array of connect request parameters
     */
    private void handleConnect(String[] tokens) {
        if (tokens.length == 3) {
            try {
                String address = tokens[1];
                int port = Integer.parseInt(tokens[2]);
                connect(address, port);
            } catch (NumberFormatException nfe) {
                printError("Port must be a number.");
                logger.error("Unable to parse port.");
            }
        } else {
            printError("Invalid number of parameters.");
        }
    }

    /**
     * Establishes connection to the storage server using {@link CommInterface CommInterface}.
     *
     * @param address IP address of the storage server
     * @param port    port number of the storage server
     * @see Store#connect()
     */
    private void connect(String address, int port) {
        if (store != null) {
            printError(String.format(
                    "Already connected to address %s and port %d. To establish new connection, disconnect first.",
                    serverAddress, serverPort));
            return;
        }

        logger.debug(String.format("Initiating connection to the server with address %s and port %d.",
                address, port));

        try {
            CommInterface newStore = new Store(address, port);
            newStore.connect();
            store = newStore;
            serverAddress = address;
            serverPort = port;
        } catch (UnknownHostException e) {
            printError(String.format("Received invalid server address %s.", e.getMessage()));
            return;
        } catch (IOException e) {
            printError(String.format("Received I/O error. %s.", e.getMessage()));
            resetConnection();
            return;
        } catch (SecurityException e) {
            printError(String.format("Received security manager error. %s.", e.getMessage()));
            return;
        } catch (IllegalArgumentException e) {
            printError(String.format("Received invalid server port number %s.", e.getMessage()));
            return;
        } catch (Exception e) {
            printError(String.format("Failed to establish connection. Error: %s.", e.getMessage()));
            return;
        }

        // Successful connection
        logger.debug(String.format("Established connection to the server with address %s and port %d.",
                address, port));
        printToPrompt(String.format("Successfully connected to server at address %s and port %d.",
                serverAddress, serverPort));
    }

    /**
     * Handles the client PUT request to the storage server.
     *
     * @param tokens array of PUT request parameters
     */
    private void handlePut(String[] tokens) {
        String key;
        String value;
        switch (tokens.length) {
            case 0:
            case 1:
            case 2:
                printError("Invalid number of parameters.");
                break;
            case 3:
                key = tokens[1];
                value = tokens[2];

                // Delete is called for null and empty values, else PUT is called
                if (value.equals("null") || value.equals("") || value.isEmpty()) {
                    delete(key);
                } else {
                    put(key, value);
                }
                break;
            default:
                key = tokens[1];
                value = tokens[2];

                // Spaced delimited values are merged into one string
                for (int i = 3; i < tokens.length; i++) {
                    value += " ";
                    value += tokens[i];
                }
                put(key, value);
        }
    }

    /**
     * Performs the PUT request using {@link CommInterface CommInterface}.
     *
     * @param key   PUT request key
     * @param value PUT request value
     * @see CommInterface#put(String, String)
     */
    private void put(String key, String value) {
        if (store == null) {
            printError("Cannot perform PUT. Not connected to any server.");
            return;
        }

        KVMessage message;
        logger.debug(String.format("Performing PUT request with key %s and value %s.", key, value));

        // Perform Put
        try {
            message = store.put(key, value);
        } catch (IllegalArgumentException e) {
            logger.debug(String.format("Failed PUT request with key %s and value %s. Invalid arguments received.", key, value));
            printError(String.format("Invalid arguments received. %s.", e.getMessage()));
            return;

        } catch (IOException e) {
            logger.debug(String.format("Failed PUT request with key %s and value %s. I/O Exception: %s",
                    key, value, e.getMessage()));
            printError(String.format("Received I/O error. %s.", e.getMessage()));
            resetConnection();
            return;

        } catch (Exception e) {
            logger.error(String.format("Failed PUT with key %s and value %s.", key, value), e);
            printError(String.format("Encountered exception while performing PUT with key %s and value %s. %s.",
                    key, value, e.getMessage()));
            return;
        }

        // Parse response
        logger.debug(String.format("Received PUT reply from server: %s ", message.toString()));
        StatusType status = message.getStatus();

        if (status.equals(StatusType.PUT_SUCCESS)) {
            printToPrompt(String.format("SUCCESS: key=%s, value=%s", message.getKey(), message.getValue()));

        } else if (status.equals(StatusType.PUT_UPDATE)) {
            printToPrompt(String.format("SUCCESS UPDATED: key=%s, value=%s", message.getKey(), message.getValue()));

        } else if (status.equals(StatusType.PUT_ERROR)) {
            printError(String.format("Failed to perform PUT with key %s and value %s.", key, value));

        } else if (status.equals(StatusType.SERVER_STOPPED)) {
            printToPrompt(String.format("Server is not open to client requests"));
        } else {
            logger.error(String.format("PUT reply with invalid status code %s", status.toString()));
            printError(String.format("Failed to perform PUT. Received invalid status code %s.", status.toString()));
        }

    }

    /**
     * Performs a DELETE request utilizing PUT request from {@link CommInterface CommInterface}.
     *
     * @param key key to be deleted
     * @see CommInterface#put(String, String)
     */
    private void delete(String key) {
        if (store == null) {
            printError("Cannot perform DELETE. Not connected to any server.");
            return;
        }

        logger.debug(String.format("Performing DELETE request with key %s.", key));
        KVMessage message;
        try {
            message = store.put(key, null);
        } catch (IllegalArgumentException e) {
            logger.debug(String.format("Failed DELETE request with key %s. Illegal args received.", key));
            printError(String.format("Invalid arguments received for DELETE with key %s. %s.",
                    key, e.getMessage()));
            return;

        } catch (IOException e) {
            logger.debug(String.format("Failed DELETE request with key %s. I/O error: %s", key, e.getMessage()));
            printError(String.format("Received I/O error. %s.", e.getMessage()));
            resetConnection();
            return;

        } catch (Exception e) {
            logger.error(String.format("Failed DELETE request with key %s.", key), e);
            printError(String.format("Encountered exception while performing DELETE with key %s. %s.",
                    key, e.getMessage()));
            return;
        }

        logger.debug(String.format("Received DELETE reply from server: %s", message.toString()));
        StatusType status = message.getStatus();
        if (status.equals(StatusType.DELETE_SUCCESS)) {
            printToPrompt(String.format("SUCCESS: key=%s", message.getKey()));

        } else if (status.equals(StatusType.DELETE_ERROR)) {
            printError(String.format("Failed to perform DELETE with key %s.", key));

        } else if (status.equals(StatusType.SERVER_STOPPED)) {
            printError(String.format("Server is not open to client requests"));
        } else {
            logger.error(String.format("Delete reply with invalid status code %s", status.toString()));
            printError(String.format("Failed to perform DELETE with key %s. Received invalid status code %s.",
                    key, status.toString()));
        }
    }

    /**
     * Handles exit of client application command line shell.
     */
    private void handleQuit() {
        if (store != null) {
            disconnect();
        }
        stop = true;
        listeningRunnable.close();
        printToPrompt("Application exit!");
    }

    /**
     * Resets the current connection by disconnecting from the storage server.
     */
    private void resetConnection() {
        if (store == null) {
            return;
        }

        store.disconnect();
        store = null;
        printToPrompt("Connection disconnected. Please reconnect to server.");
    }

    /**
     * Disconnects the current storage server.
     */
    private void disconnect() {
        if (store == null) {
            printError("Cannot disconnect. Not connected to any server.");
            return;
        }

        logger.debug("Disconnecting from server.");

        store.disconnect();
        logger.debug("Successfully disconnected.");
        printToPrompt("Successfully disconnected.");
        store = null;
    }

    /**
     * Prints help text to console.
     */
    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("CLIENT HELP (Usage):\n\n");

        sb.append(PROMPT).append("connect <address> <port>");
        sb.append("\t establishes a connection to a server\n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t insert a key value pair to the storage server \n");

        sb.append(PROMPT).append("put <key> null");
        sb.append("\t\t delete a key value pair from the storage server \n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieves the value for the given key from the storage server \n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t\t changes the logLevel to the specified log level \n");

        sb.append(PROMPT);
        sb.append("\t\t\t\t ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("help");
        sb.append("\t\t\t\t displays this help prompt\n");

        sb.append(PROMPT).append("subscribe <key>");
        sb.append("\t\t subscribe to a key\n");

        sb.append(PROMPT).append("unsubscribe <key>");
        sb.append("\t\t unsubscribe from a key\n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t exits the program\n");
        sb.append("::::::::::::::::::::::::::::::::\n");
        System.out.println(sb.toString());
    }

    /**
     * Handles the logLevel request from client.
     *
     * @param tokens array of logLevel request parameters
     */
    private void handleLogLevel(String[] tokens) {
        if (tokens.length == 2) {
            parseLogLevelParams(tokens[1]);
        } else {
            printError("Invalid number of parameters.");
        }
    }

    /**
     * Parses the log level from the logLevel request.
     *
     * @param token new log level
     */
    private void parseLogLevelParams(String token) {
        String level = setLevel(token);
        if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
            printError("No valid log level.");
            printPossibleLogLevels();
        } else {
            printToPrompt("Log level changed to level " + level);
        }
    }

    /**
     * Prints all supported log levels.
     */
    private void printPossibleLogLevels() {
        printToPrompt("Possible log levels are:");
        printToPrompt("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    /**
     * Sets the client logging level to new levelString.
     *
     * @param levelString new logging level
     * @return new logging level
     */
    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    /**
     * Prints an error message to console.
     *
     * @param error error message
     */
    private void printError(String error) {
        System.out.println("Error: " + error);
    }

    /**
     * Prints a regular message to console.
     *
     * @param msg message
     */
    private void printToPrompt(String msg) {
        System.out.println(msg);
    }

    /**
     * Main entry point for the  Client application.
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ERROR, "");
            Client app = new Client();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
