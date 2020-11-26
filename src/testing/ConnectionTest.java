package testing;

import client.Store;
import junit.framework.TestCase;

import java.net.UnknownHostException;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		Store client = new Store("127.0.0.1", 50000);
		try {
			client.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		Store client = new Store("unknown", 50000);
		
		try {
			client.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		Store client = new Store("127.0.0.1", 123456789);
		
		try {
			client.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

