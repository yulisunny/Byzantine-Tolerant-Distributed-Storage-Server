package app_server;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Entry point for Server.
 */
public class Server {

    private static final Level DEFAULT_LOG_LEVEL = Level.ALL;
    private static final String LOG_DIR = "logs/server.log";
    private static Logger logger = Logger.getRootLogger();

    /**
     * Start KV Server at given port
     * @param port given port for storage server to operate
     */
    //    public Server(int port, int cacheSize, String strategy) {
    //        new WelcomeThread(port, cacheSize, strategy).start();
    //    }
    public Server(String ipAddr, int port) {
        new WelcomeThread(ipAddr, port).start();
    }

    /**
     * Starts up the server by passing args to main.
     *
     * @param args contains port number, log level #contains port number, cache size, strategy and optionally log level
     */
    public static void main(String[] args) {
        Level logLevel = DEFAULT_LOG_LEVEL;
        switch (args.length) {
            case 2:
                break;
            case 3:
                if (LogSetup.isValidLevel(args[2])) {
                    logLevel = Level.toLevel(args[2]);
                } else {
                    System.out.println("Error! Log level not supported!");
                    System.out.println("Supported log levels: " + LogSetup.getPossibleLogLevels());
                    System.exit(1);
                }
                break;
            default:
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server ip_addr port!");
                System.exit(1);
        }


        String ipAddr = args[0];

        // Parse port
		int port = 0;
		try {
			port = Integer.parseInt(args[1]);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server port cache_size cache_strategy [log_level]!");
			System.exit(1);
		}

        // Set up logs
        try {
            new LogSetup(LOG_DIR, logLevel, "Port: " + port);
        } catch (IOException e) {
            System.out.println("Failed to create log file at " + LOG_DIR + ". Exiting now...");
            System.exit(1);
        }

        // Parse cache size
        //		int cacheSize = 0;
        //		try {
        //			cacheSize = Integer.parseInt(args[1]);
        //		} catch (NumberFormatException nfe) {
        //			System.out.println("Error! Invalid argument <cache_size>! Not a number!");
        //			System.out.println("Usage: Server port cache_size cache_strategy [log_level]!");
        //			System.exit(1);
        //		}
        //		if (cacheSize < 0) {
        //			System.out.println("Error! Invalid argument <cache_size>! Must be a natural number!");
        //			System.out.println("Usage: Server port cache_size cache_strategy [log_level]!");
        //			System.exit(1);
        //		}

        // Parse cache strategy
        //		String strategy;
        //        strategy = args[2];

        // Start up a new thread
        logger.info("Starting server on port " + port);
        new Server(ipAddr, port); //, cacheSize, strategy);
    }

}
