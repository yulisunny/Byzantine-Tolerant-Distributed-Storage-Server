package testing;

import app_admin.AdminStore;
import client.Store;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest extends TestCase {

    @Test
    public void testPerformance() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR, "");
        } catch (IOException e) {
            e.printStackTrace();
        }

        int numServers = 10;
        int cacheSize = 1000;
        int numClient = 100;

        AdminStore adminStore = new AdminStore();
        try {
            List<String> keys = new ArrayList<>();

            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(new File("keys")));
            } catch (IOException e) {
                e.printStackTrace();
            }

            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String key = currentLine.trim();
                keys.add(key);
            }
            reader.close();

            adminStore.initService(numServers, cacheSize, "FIFO");
            adminStore.start();
            Thread.sleep(1000);

            List<Thread> clients = new ArrayList<>();
            for (int i = 0; i < numClient; i++) {
                Thread clientThread = new Thread(new ClientRunnable(keys));
                clientThread.start();
                clients.add(clientThread);
            }

            for (Thread client : clients) {
                client.join();
            }

            adminStore.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testAddRmNode() {
        int numServers = 100;
        int cacheSize = 1000;

        try {
            new LogSetup("logs/testing/test.log", Level.ERROR, "");
        } catch (IOException e) {
            e.printStackTrace();
        }

        AdminStore adminStore = new AdminStore();
        try {
            adminStore.initService(numServers, cacheSize, "FIFO");
            adminStore.start();
            Thread.sleep(1000);

            long start = System.currentTimeMillis();
            adminStore.removeNode(0, false);
            double elapsed = ((double) (System.currentTimeMillis() - start))/1000.0;
            System.out.println("Remove 1 node: " + elapsed + "ms");

            Thread.sleep(1000);

            start = System.currentTimeMillis();
            adminStore.addNode(cacheSize, "FIFO");
            elapsed = ((double) (System.currentTimeMillis() - start))/1000.0;
            System.out.println("Add 1 node: " + elapsed + "ms");

            Thread.sleep(1000);

            start = System.currentTimeMillis();
            adminStore.removeNode(0, false);
            adminStore.removeNode(1, false);
            adminStore.removeNode(2, false);
            adminStore.removeNode(3, false);
            adminStore.removeNode(4, false);
            elapsed = ((double) (System.currentTimeMillis() - start))/1000.0;
            System.out.println("Remove 5 nodes: " + elapsed + "ms");

            Thread.sleep(1000);

            start = System.currentTimeMillis();
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            elapsed = ((double) (System.currentTimeMillis() - start))/1000.0;
            System.out.println("Add 5 nodes: " + elapsed + "ms");

            Thread.sleep(1000);

            start = System.currentTimeMillis();
            adminStore.removeNode(0, false);
            adminStore.removeNode(1, false);
            adminStore.removeNode(2, false);
            adminStore.removeNode(3, false);
            adminStore.removeNode(4, false);
            adminStore.removeNode(5, false);
            adminStore.removeNode(6, false);
            adminStore.removeNode(7, false);
            adminStore.removeNode(8, false);
            adminStore.removeNode(9, false);
            elapsed = ((double) (System.currentTimeMillis() - start))/1000.0;
            System.out.println("Remove 10 nodes: " + elapsed + "ms");

            Thread.sleep(1000);

            start = System.currentTimeMillis();
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            adminStore.addNode(cacheSize, "FIFO");
            elapsed = ((double) (System.currentTimeMillis() - start))/1000.0;
            System.out.println("Add 10 nodes: " + elapsed + "ms");

            Thread.sleep(1000);
            adminStore.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPopulate() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR, "");
        } catch (IOException e) {
            e.printStackTrace();
        }

        populateData(50, 1000);
    }

    private void populateData(int numServers, final int numKeys) {
        AdminStore adminStore = new AdminStore();
        final Store populationClient = new Store("127.0.0.1", 50000);
        try {
            adminStore.initService(numServers, 10000, "FIFO_UNIQUE");
            Thread.sleep(5000);
            adminStore.start();
            Thread.sleep(20000);
            populationClient.connect();

            Path p = Paths.get("maildir");
            final AtomicInteger numWritten = new AtomicInteger(0);
            final File keysFile = new File("keys");
            keysFile.delete();
            keysFile.createNewFile();


            FileVisitor<Path> fv = new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getNameCount() == 4) {
                        try {
                            String key = file.subpath(1, 4).toString();

                            BufferedWriter writer = null;
                            try {
                                writer = new BufferedWriter(new FileWriter(keysFile, true));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            try {
                                writer.write(key + System.getProperty("line.separator"));
                                writer.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            BufferedReader reader = new BufferedReader(new FileReader(file.toFile()));
    //                        String fileString = new String(Files.readAllBytes(file));
                            String fileString = reader.readLine().trim();
    //                        fileString = fileString.replace("\n", " ").replace("\r", " ");
                            populationClient.put(key, fileString);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        int num = numWritten.addAndGet(1);
                        if (num % 100 == 0) {
                            System.out.println("num = " + num);
                        }
                        if (num >= numKeys) {
                            return FileVisitResult.TERMINATE;
                        }
                    }


                    return FileVisitResult.CONTINUE;
                }
            };

            try {
                Files.walkFileTree(p, fv);
            } catch (IOException e) {
                e.printStackTrace();
            }

            populationClient.disconnect();

            Thread.sleep(10000);
            adminStore.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class ClientRunnable implements Runnable {

        private List<String> keys;

        public ClientRunnable(List<String> keys) {
            this.keys = keys;
        }

        @Override
        public void run() {
            try {
                Store client = new Store("127.0.0.1", 50000);
                client.connect();

                long start = System.currentTimeMillis();
                for (int i = 0; i < keys.size(); i++) {
                    if (i % 500 == 0) {
                        System.out.println("Read " + i);
                    }
                    client.get(keys.get(i));
                }
                long elapsed = (System.currentTimeMillis() - start);
                System.out.println("Read elapsed = " + elapsed + "ms");
                System.out.println(((double) elapsed)/keys.size() + "ms per read.");

                start = System.currentTimeMillis();
                for (int i = 0; i < keys.size(); i++) {
                    if (i % 500 == 0) {
                        System.out.println("Write " + i);
                    }
                    client.put(keys.get(i), "performance_test");
                }
                elapsed = (System.currentTimeMillis() - start);
                System.out.println("Write elapsed = " + elapsed + "ms");
                System.out.println(((double) elapsed)/keys.size() + "ms per write.");

                client.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
