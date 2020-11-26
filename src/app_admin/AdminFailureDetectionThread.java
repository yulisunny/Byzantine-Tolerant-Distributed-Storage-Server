package app_admin;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AdminFailureDetectionThread extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private static int port = 61034; // special port number allocated just for server
    private ServerSocket serverSocket;
    private boolean running = true;
    private AdminCommInterface store;

    public AdminFailureDetectionThread(AdminCommInterface store)
    {
        this.store = store;
    }

    public static ReentrantLock mutexHandleFailureRecovery = new ReentrantLock(true);

    public static HashMap<String, String> serverCompromisedCountMap = new HashMap<>();

    @Override
    public void run() {
        running = bootupAdminServer();

        if (serverSocket != null) {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    AdminClientConnection connection = new AdminClientConnection(client, store);
                    new Thread(connection).start();
                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.debug("serverSocket exception");
                    running = false;
                    break;
                }
            }
        }
        logger.info("Server stopped.");
    }

    public void close() {
        running = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error("Error! " +
                        "Unable to close admin server socket on port: " + port, e);
            }
        }
    }
    private boolean bootupAdminServer() {
        try {
            serverSocket = new ServerSocket(port);
            return true;

        } catch (IOException e) {
            logger.debug("Admin: Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        } catch (IllegalArgumentException e) {
            logger.error("Admin: Error! Cannot open server socket: " + e.getMessage());
            return false;
        }
    }


}
