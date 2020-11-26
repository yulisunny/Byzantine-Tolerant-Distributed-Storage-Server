package app_admin;

import adminclient.AdminCommInterface;
import adminclient.AdminStore;
import app_server.WelcomeThread;
import common.HashRange;
import common.Metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AdminClientForOneServer {

    private static final String PROMPT = "KVAdminClient> ";

    private BufferedReader stdin;
    private AdminCommInterface store;;
    private Boolean stop = false;
    private Metadata metadata;

    /**
     * Initiates the command line shell and handle client requests.
     */
    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            Metadata serverAMetaData = new Metadata();
            HashRange hashrange = new HashRange("00000000000000000000000000000000", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

            serverAMetaData.setHashRange("localhost", 45000, hashrange);
            metadata = serverAMetaData;

            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
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

        if (tokens[0].equals("initKVServer")) {
            handleInitKVServer(tokens);
        } else if (tokens[0].equals("start")) {
            handleStart();
        } else if (tokens[0].equals("stop")) {
            handleStop();
        } else if (tokens[0].equals("lockWrite")) {
            handleLockwrite();
        } else if (tokens[0].equals("unlockWrite")) {
            handleUnlockwrite();
        } else if (tokens[0].equals("moveData")) {
            handleMoveData(tokens);
        } else if (tokens[0].equals("connect")) {
            handleConnect(tokens);
        }
        else if (tokens[0].equals("hash")) {
            System.out.println(WelcomeThread.getMd5Hash(tokens[1]));

            if (metadata.getHashRange("localhost", 40000).isInRange(WelcomeThread.getMd5Hash(tokens[1]))) {
                System.out.println("HIHIHI");
            }
            else {

                System.out.println("HIHswswswswwsIHI");
            }


            if (metadata.getHashRange("localhost", 40000).isInRange("10001111111100001111000011110000")) {
                System.out.println("HIHIHId");
            }
            else {
                System.out.println("HIHswswswswwsIHId");
                System.out.println("00000000".compareTo("01101111"));
            }
        }
        else {
            printError("Unknown command.");
        }
    }

    private void handleConnect(String[] tokens) {
        try {
            String address = tokens[1];
            int port  = Integer.parseInt(tokens[2]);
            store = new AdminStore(address, port);
            store.connect();
        } catch (Exception e) {

        }

    }

    private void handleInitKVServer(String[] tokens) {
        if (tokens.length == 3) {
            try {
                String address = tokens[1];
                int port  = Integer.parseInt(tokens[2]);
                store = new AdminStore(address, port);

                store.initKVServer(50, "FIFO", metadata);

            } catch (Exception e) {
            }
        } else {
            printError("Invalid number of parameters.");
        }
    }

    private void handleStart() {
        try {
            store.start();
        } catch (Exception e) {

        }
    }

    private void handleStop() {
        try {
            store.stop();
        } catch (Exception e) {

        }
    }

    private void handleLockwrite() {
        try {
            store.lockWrite();
        } catch (Exception e) {

        }
    }
    private void handleUnlockwrite() {
        try {
            store.unLockWrite();
        } catch (Exception e) {

        }
    }

    private void handleMoveData(String[] tokens) {
        try {
            Metadata metadata = new Metadata();
            HashRange hashRange = new HashRange(tokens[3], tokens[4]);
            metadata.setHashRange(tokens[1], Integer.valueOf(tokens[2]), hashRange);
            store.moveData(tokens[1], tokens[2], metadata);
        } catch (Exception e) {

        }
    }
    private void printError(String error) {
        System.out.println("Error: " + error);
    }

    /**
     * Main entry point for the  Client application.
     */
    public static void main(String[] args) {
        AdminClientForOneServer app = new AdminClientForOneServer();
        app.run();
    }

}