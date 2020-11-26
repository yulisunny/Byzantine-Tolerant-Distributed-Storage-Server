package client;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ClientListenRunnable implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    public static Map<String, String> reports = new HashMap<>();

    private boolean running = true;
    private static ServerSocket listeningSocket;

    @Override
    public void run() {
        running = bootUpClientListen();

        if (listeningSocket != null) {
            while (running) {
                try {
                    Socket server = listeningSocket.accept();
                    ClientServerConnection connection = new ClientServerConnection(server);
                    new Thread(connection).start();
                    logger.info("Connected to a server "
                            + server.getInetAddress().getHostAddress()
                            + " on port " + server.getPort());
                } catch (IOException e) {
                    logger.debug("listening socket accept exception, maybe socket closed");
                    running = false;
                    break;
                }
            }
        }
    }

    public void close() {
        running = false;
        if (listeningSocket != null) {
            try {
                listeningSocket.close();
            } catch (IOException e) {
                logger.error("Error! Unable to close listening socket", e);
            }
        }
    }

    private boolean bootUpClientListen() {
        try {
            listeningSocket = new ServerSocket();
            listeningSocket.bind(listeningSocket.getLocalSocketAddress());
            logger.info("Listening on socket " + listeningSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.debug("Client listening port creation failed");
            return false;
        }

    }

    public static int getPortNumber() {
        try {
            return listeningSocket.getLocalPort();
        } catch (Exception e) {
            return 1;
        }

    }
}
