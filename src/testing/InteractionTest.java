package testing;

import client.Store;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import org.junit.Test;


public class InteractionTest extends TestCase {

	private Store client;
	
	public void setUp() {
		client = new Store("127.0.0.1", 50000);
		try {
			client.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		client.disconnect();
	}
	
	
	@Test
	public void testPut() {
		String key = "putfoo";
		String value = "putbar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = client.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE);
	}
	
	@Test
	public void testPutDisconnected() {
		client.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			client.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			client.put(key, initialValue);
			Thread.sleep(1000);
			response = client.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null);
		assertEquals(StatusType.PUT_UPDATE, response.getStatus());
		assertTrue(response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			client.put(key, value);
			response = client.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				client.put(key, value);
				response = client.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = client.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	


}
