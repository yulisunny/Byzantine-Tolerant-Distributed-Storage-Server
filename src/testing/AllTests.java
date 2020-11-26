package testing;

import adminclient.AdminStore;
import app_server.Server;
import common.HashRange;
import common.Metadata;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR, "");
			//use a default of 128 keys, FIFO replacement
			//new Server(50000, 128, "FIFO");
			Server server = new Server("127.0.0.1", 50000);

            Metadata metadata = new Metadata();
            metadata.setHashRange("127.0.0.1", 50000,
					new HashRange("00000000000000000000000000000000", "00000000000000000000000000000000","00000000000000000000000000000000"));

            AdminStore adminStore = new AdminStore("127.0.0.1", 50000);
            try {
                //adminStore.connect();
                adminStore.initKVServer(100, "FIFO", metadata);
                adminStore.start();
            } catch (Exception e) {
                System.out.println(e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
//		clientSuite.addTestSuite(PerformanceTest.class);
		return clientSuite;
	}
	
}
