package app_admin;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Command line support for External Configuration Service.
 */

public class AdminClient implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "AdminClient> ";

    private BufferedReader stdin;
    private Boolean stop = false;
    private AdminCommInterface store;

    public static int cacheSize;
    public static String cacheStrategy;

    private AdminFailureDetectionThread adminFailureDetection;


    /**
     * Initiates the command line shell and handle client requests.
     */
    @Override
    public void run() {
        store = new AdminStore();
        adminFailureDetection = new AdminFailureDetectionThread(store);
        adminFailureDetection.start();
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                adminFailureDetection.close();
                printError("CLI does not respond - Application terminated ");
            }
        }
        try {
            adminFailureDetection.join(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("Exiting application");
        System.out.println("Exiting Application.");
    }

    /**
     * Entry point for the Admin client.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/admin.log", Level.ALL, "");
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        AdminClient app = new AdminClient();
        app.run();
    }

    /**
     * Parses and initiate calls to handlers for each request.
     *
     * @param cmdLine client request string
     */
    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("initService")) {
            handleInitService(tokens);
        } else if (tokens[0].equals("start")) {
            handleStart();
        } else if (tokens[0].equals("stop")) {
            handleStop();
        } else if (tokens[0].equals("shutDown")) {
            handleShutDown();
        } else if (tokens[0].equals("addNode")) {
            handleAddNode(tokens);
        } else if (tokens[0].equals("removeNode")) {
            handleRemoveNode(tokens);
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else if (tokens[0].equals("quit")) {
            handleQuit();
        } else {
            printError("Unknown command.");
        }
    }

    private void handleInitService(String[] tokens) {
        if (tokens.length == 4) {
            try {
                int numberOfNodes = Integer.parseInt(tokens[1]);
                cacheSize = Integer.parseInt(tokens[2]);
                cacheStrategy = tokens[3];
                store.initService(numberOfNodes, cacheSize, cacheStrategy);
            } catch (NumberFormatException e) {
                logger.error("Unable to parse number of nodes or cache size.");
                printError("Number of nodes and cache size must be numbers.");
            } catch(IllegalArgumentException e){
                logger.error("Received illegal arguments. Please initialize at least 3 servers.");
                printError("Received illegal arguments. Please initialize at least 3 servers.");
            } catch (IOException e) {
                logger.error("Unable to read config file.");
                printError("Cannot read config file.");
            } catch (Exception e) {
                logger.error("Encountered error while initializing.", e);
                printError("Encountered error while initializing");
            }

        } else {
            printError("Invalid number of parameters. Need numberOfNodes, cacheSize and replacementStrategy.");
        }
    }

    private void handleStart() {
        try {
            store.start();
        } catch(Exception e) {
            logger.info("Start node failed", e);
            printError("Operation was unsuccessful. " + e.getMessage());
        }
    }

    private void handleStop() {
        try {
            store.stop();
        } catch(Exception e) {
            logger.info("Stop node failed", e);
            printError("Operation was unsuccessful. " + e.getMessage());
        }
    }

    private void handleShutDown() {
        try {
            store.shutDown();
        } catch(Exception e) {
            logger.info("Shutdown node failed", e);
            printError("Operation was unsuccessful. " + e.getMessage());
        }
    }

    private void handleAddNode(String[] tokens) {
        if (tokens.length == 3) {
            try {
                int cacheSize = Integer.parseInt(tokens[1]);
                store.addNode(cacheSize, tokens[2]);
            } catch (NumberFormatException e) {
                printError("Cache size must be a number.");
                logger.error("Unable to parse cache size.");
            } catch (IllegalStateException e) {
                printError(e.getMessage());
                logger.error("Received exception when adding node.", e);
            } catch (Exception e) {
                printError(e.getMessage());
                logger.error("Received exception when adding node.", e);
            }
        } else {
            printError("Invalid number of parameters. Need cacheSize and replacementStrategy.");
        }
    }

    private void handleRemoveNode(String[] tokens) {
        if (tokens.length == 2) {
            try {
                int serverIndex = Integer.parseInt(tokens[1]);
                store.removeNode(serverIndex, false);
            } catch (NumberFormatException e) {
                printError("Index of server must be a number.");
                logger.error("Unable to parse index of server.");
            } catch(IllegalStateException e) {
                printError(e.getMessage());
                logger.error("Cannot remove node when there are 3 or less servers running.", e);
            } catch(Exception e) {
                printError("Operation was unsuccessful.");
                logger.error("Remove node failed", e);
            }
        } else {
            printError("Invalid number of parameters. Need index of server.");
        }
    }

    /**
     * Handles exit of client application command line shell.
     */
    private void handleQuit() {
        if (store != null) {
            try {
                store.shutDown();
            } catch (Exception e) {
                printError("Operation was unsuccessful.");
                logger.error("Remove node failed", e);
            }
        }
        adminFailureDetection.close();
        stop = true;
    }

    /**
     * Prints help text to console.
     */
    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("AdminClint HELP (Usage):\n\n");

        sb.append(PROMPT).append("initService <numberOfNodes> <cacheSize> <cacheStrategy>\n");
        sb.append("\t\t Initialize numberOfNodes amount of Server with a cache size of cacheSize\n");

        sb.append(PROMPT).append("start");
        sb.append("\t\t Start all initialized KVServers \n");

        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stop all running KVservers from processing client requests\n");

        sb.append(PROMPT).append("shutDown");
        sb.append("\t\t Terminate all KVservers processes\n");

        sb.append(PROMPT).append("addNode <cacheSize> <cacheStrategy>");
        sb.append("\t\t\t Initialize one Server with a cache size of cacheSize\n");

        sb.append(PROMPT).append("removeNode");
        sb.append("\t\t\t Randomly removes a node\n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t\t changes the logLevel to the specified log level \n");

        sb.append(PROMPT);
        sb.append("\t\t\t\t\t ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("help");
        sb.append("\t\t\t\t displays this help prompt\n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t exits the program\n");
        sb.append("::::::::::::::::::::::::::::::::\n");
        System.out.println(sb.toString());
    }

    private void printError(String error) {
        System.out.println("Error: " + error);
    }

}