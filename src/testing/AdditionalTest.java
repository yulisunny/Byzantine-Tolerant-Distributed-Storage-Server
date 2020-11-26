package testing;

import app_admin.AdminStore;
import app_server.AbstractCachedStorage;
import app_server.FifoCachedStorage;
import app_server.LfuCachedStorage;
import app_server.LruCachedStorage;
import client.Store;
import common.HashRange;
import common.Metadata;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AdditionalTest extends TestCase {

    private static Logger logger = Logger.getRootLogger();

    @Test
    public void testReplication() {
        try {
            AdminStore adminStore = new AdminStore();
            adminStore.initService(3, 10, "LRU");
            adminStore.start();

            List<String> running = adminStore.getRunningServers();
            String serverKey1 = running.get(0);
            String serverKey2 = running.get(1);
            String serverKey3 = running.get(2);
            String[] items1 = serverKey1.split(":");
            String[] items2 = serverKey1.split(":");
            String[] items3 = serverKey1.split(":");

            Store kvStore1 = new Store(items1[0], Integer.parseInt(items1[1]));
            Store kvStore2 = new Store(items2[0], Integer.parseInt(items2[1]));
            Store kvStore3 = new Store(items3[0], Integer.parseInt(items3[1]));
            kvStore1.connect();
            kvStore2.connect();
            kvStore3.connect();

            kvStore1.put("a", "b");
            KVMessage retMesg1 = kvStore1.get("a");
            assertEquals(KVMessage.StatusType.GET_SUCCESS, retMesg1.getStatus());
            assertEquals("b", retMesg1.getValue());

            Thread.sleep(2000);

            KVMessage retMesg2 = kvStore2.get("a");
            assertEquals(KVMessage.StatusType.GET_SUCCESS, retMesg2.getStatus());
            assertEquals("b", retMesg2.getValue());

            KVMessage retMesg3 = kvStore3.get("a");
            assertEquals(KVMessage.StatusType.GET_SUCCESS, retMesg3.getStatus());
            assertEquals("b", retMesg3.getValue());

            kvStore1.disconnect();
            kvStore2.disconnect();
            kvStore3.disconnect();
            adminStore.shutDown();

        } catch (Exception e) {
        }

    }

    @Test
    public void testHashRangeReadIsInRange() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host4 = "127.0.0.1:50003";

        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        String host4Hash = "A98109598267087DFC364FAE4CF24578";

        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));
        assertTrue(metadata.getHashRange(host1).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));
        assertTrue(metadata.getHashRange(host2).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));
        assertTrue(metadata.getHashRange(host3).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));

        // 1 -> 4 -> 3 -> 2
        metadata.addNewServer(host4, host4Hash);
        assertFalse(metadata.getHashRange(host2).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));
        assertTrue(metadata.getHashRange(host3).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));
        assertTrue(metadata.getHashRange(host4).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));
        assertTrue(metadata.getHashRange(host1).isInReadRange("Z3638A32C297F43AA37E63BBD839FC7E"));
    }

    @Test
    public void testMetadataAddServerReturnedUpdateRange() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host4 = "127.0.0.1:50003";
        String host5 = "127.0.0.1:50004";

        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        String host4Hash = "A98109598267087DFC364FAE4CF24578";
        String host5Hash = "DA850509FC3B88A612B0BCAD7A37963B";

        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        // adding one server
        Map<String, HashRange> ret = metadata.addNewServer(host4, host4Hash);
        assertEquals(3, ret.size());
        assertTrue(ret.containsKey(host1));
        assertEquals(host1Hash, ret.get(host1).getStartHash());
        assertEquals(host4Hash, ret.get(host1).getEndHash());

        assertTrue(ret.containsKey(host2));
        assertEquals(host2Hash, ret.get(host2).getStartHash());
        assertEquals(host1Hash, ret.get(host2).getEndHash());

        assertTrue(ret.containsKey(host3));
        assertEquals(host3Hash, ret.get(host3).getStartHash());
        assertEquals(host2Hash, ret.get(host3).getEndHash());

        // adding one more server
        ret = metadata.addNewServer(host5, host5Hash);
        assertEquals(3, ret.size());
        assertTrue(ret.containsKey(host2));
        assertEquals(host1Hash, ret.get(host2).getStartHash());
        assertEquals(host4Hash, ret.get(host2).getEndHash());

        assertTrue(ret.containsKey(host1));
        assertEquals(host4Hash, ret.get(host1).getStartHash());
        assertEquals(host3Hash, ret.get(host1).getEndHash());

        assertTrue(ret.containsKey(host4));
        assertEquals(host3Hash, ret.get(host4).getStartHash());
        assertEquals(host5Hash, ret.get(host4).getEndHash());
    }

    @Test
    public void testMetadataAddServerReadStartHashUpdate() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host4 = "127.0.0.1:50003";
        String host5 = "127.0.0.1:50004";

        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        String host4Hash = "A98109598267087DFC364FAE4CF24578";
        String host5Hash = "DA850509FC3B88A612B0BCAD7A37963B";

        // Initialize 3 servers
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        // adding one server, now have 4 servers
        metadata.addNewServer(host4, host4Hash);
        assertEquals(host4Hash, metadata.getHashRange(host1).getReadStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host2).getReadStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host2Hash, metadata.getHashRange(host3).getReadStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host4).getReadStartHash());
        assertEquals(host4Hash, metadata.getHashRange(host4).getEndHash());

        // adding one server, now have 5 servers
        metadata.addNewServer(host5, host5Hash);
        assertEquals(host3Hash, metadata.getHashRange(host1).getReadStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host4Hash, metadata.getHashRange(host2).getReadStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host2Hash, metadata.getHashRange(host3).getReadStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
        assertEquals(host5Hash, metadata.getHashRange(host4).getReadStartHash());
        assertEquals(host4Hash, metadata.getHashRange(host4).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host5).getReadStartHash());
        assertEquals(host5Hash, metadata.getHashRange(host5).getEndHash());
    }

    @Test
    public void testMetadataRemoveServerReturnedUpdateRange() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host4 = "127.0.0.1:50003";
        String host5 = "127.0.0.1:50004";

        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        String host4Hash = "A98109598267087DFC364FAE4CF24578";
        String host5Hash = "DA850509FC3B88A612B0BCAD7A37963B";

        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3, host4, host5));

        // removing one server from five
        Map<String, HashRange> ret = metadata.removeServer(host5);
        assertTrue(ret.containsKey(host2));
        assertEquals(host1Hash, ret.get(host2).getStartHash());
        assertEquals(host4Hash, ret.get(host2).getEndHash());

        assertTrue(ret.containsKey(host1));
        assertEquals(host4Hash, ret.get(host1).getStartHash());
        assertEquals(host3Hash, ret.get(host1).getEndHash());

        assertTrue(ret.containsKey(host4));
        assertEquals(host3Hash, ret.get(host4).getStartHash());
        assertEquals(host5Hash, ret.get(host4).getEndHash());
    }

    @Test
    public void testMetadataRemoveServerReadStartHashUpdate() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host4 = "127.0.0.1:50003";
        String host5 = "127.0.0.1:50004";

        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        String host4Hash = "A98109598267087DFC364FAE4CF24578";
        String host5Hash = "DA850509FC3B88A612B0BCAD7A37963B";

        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3, host4, host5));

        // removing one server from five
        metadata.removeServer(host5);
        assertEquals(host4Hash, metadata.getHashRange(host1).getReadStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host2).getReadStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host2Hash, metadata.getHashRange(host3).getReadStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host4).getReadStartHash());
        assertEquals(host4Hash, metadata.getHashRange(host4).getEndHash());

        // removing one server from four
        metadata.removeServer(host4);
        assertEquals(host1Hash, metadata.getHashRange(host1).getReadStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getReadStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getReadStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
    }

    @Test
    public void testMetadataReplicas() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        List<String> replicas = metadata.getReadableServers(host3Hash);
        assertTrue(replicas.contains(host1));
        assertTrue(replicas.contains(host2));
        assertTrue(replicas.contains(host3));

        assertEquals(host3, metadata.getWritableServer(host3Hash));
    }

    @Test
    public void testMetadataReadHashCreationIntegrity() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        assertTrue(metadata.hasServer(host1));
        assertTrue(metadata.hasServer(host2));
        assertTrue(metadata.hasServer(host3));

        assertEquals(host1Hash, metadata.getHashRange(host1).getReadStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getReadStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getReadStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
    }

    @Test
    public void testMetadataReadHashCreationIntegrityMultipleServers() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host4 = "127.0.0.1:50003";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        String host4Hash = "A98109598267087DFC364FAE4CF24578";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3, host4));

        assertEquals(host4Hash, metadata.getHashRange(host1).getReadStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host2).getReadStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host2Hash, metadata.getHashRange(host3).getReadStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host4).getReadStartHash());
        assertEquals(host4Hash, metadata.getHashRange(host4).getEndHash());
    }

    @Test
    public void testMetadataGetImmediateSuccessor() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        assertTrue(metadata.hasServer(host1));
        assertTrue(metadata.hasServer(host2));
        assertTrue(metadata.hasServer(host3));

        // Make sure the correct successor could be found
        String dataHash = "098f6bcd4621d373cade4e832627b4f6";
        assertEquals(host1, metadata.getImmediateSuccessor(dataHash).getValue());
        dataHash = "A98f6bcd4621d373cade4e832627b4f6";
        assertEquals(host3, metadata.getImmediateSuccessor(dataHash).getValue());
        dataHash = "C98f6bcd4621d373cade4e832627b4f6";
        assertEquals(host2, metadata.getImmediateSuccessor(dataHash).getValue());

        // Make sure this function does not return the same host
        assertEquals(host3, metadata.getImmediateSuccessor(host1Hash).getValue());
    }

	@Test
	public void testStub() {
		assertTrue(true);
	}

	@Test
    public void testMetadataSerialization() {
	    String hostname = "127.0.0.1";
	    Integer port = 1234;
        Metadata metadata = new Metadata();
        metadata.setHashRange(hostname, port, new HashRange("ABCD", "EFGH", "ABCD"));

        // Test serialization
        String serialized = metadata.getSerializedForm();
        Metadata deserialized = Metadata.deserialize(serialized);
        assertTrue(deserialized.hasServer(hostname, port));
        assertEquals(
                metadata.getHashRange(hostname, port).getStartHash(),
                deserialized.getHashRange(hostname, port).getStartHash());
        assertEquals(
                metadata.getHashRange(hostname, port).getEndHash(),
                deserialized.getHashRange(hostname, port).getEndHash());
    }

    @Test
    public void testMetadataHashCreationIntegrity() {
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        assertTrue(metadata.hasServer(host1));
        assertTrue(metadata.hasServer(host2));
        assertTrue(metadata.hasServer(host3));

        assertEquals(host2Hash, metadata.getHashRange(host1).getStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host2).getStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host3).getStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());
    }

    @Test
    public void testMetadataAddNewServer(){
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2));

        // Add new server to existing 2 servers
        metadata.addNewServer(host3, host3Hash);
        assertEquals(host2Hash, metadata.getHashRange(host1).getStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host3Hash, metadata.getHashRange(host2).getStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host3).getStartHash());
        assertEquals(host3Hash, metadata.getHashRange(host3).getEndHash());

        // Check adding server when there are no servers
        metadata = new Metadata();
        metadata.addNewServer(host1, host1Hash);
        assertEquals(host1Hash, metadata.getHashRange(host1).getStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
    }

    @Test
    public void testMetadataRemoveServer(){
        String host1 = "127.0.0.1:50000";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        String host2Hash = "DCEE0277EB13B76434E8DCD31A387709";
        String host3Hash = "B3638A32C297F43AA37E63BBD839FC7E";
        Metadata metadata = new Metadata(Arrays.asList(host1, host2, host3));

        // removing one server from three
        metadata.removeServer(host3);
        assertEquals(host2Hash, metadata.getHashRange(host1).getStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());
        assertEquals(host1Hash, metadata.getHashRange(host2).getStartHash());
        assertEquals(host2Hash, metadata.getHashRange(host2).getEndHash());

        // Remove one server with one left
        metadata.removeServer(host2);
        assertEquals(host1Hash, metadata.getHashRange(host1).getStartHash());
        assertEquals(host1Hash, metadata.getHashRange(host1).getEndHash());

        // Remove till no servers left
        metadata.removeServer(host1);
        assertFalse(metadata.hasServer(host1));
        assertFalse(metadata.hasServer(host2));
        assertFalse(metadata.hasServer(host3));

        // Make sure removal still works when there are no servers, should not throw any exception
        metadata.removeServer(host1);
    }

    @Test
    public void testMetadataSuccessor() {
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        Metadata metadata = new Metadata(Arrays.asList(host2, host3));
        assertEquals(host3, metadata.getSuccessorServer(host1Hash));
    }

    @Test
    public void testMetadataPredecessor() {
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        String host1Hash = "358343938402EBB5110716C6E836F5A2";
        Metadata metadata = new Metadata(Arrays.asList(host2, host3));
        assertEquals(host2, metadata.getPredecessorServer(host1Hash));
    }

    @Test
    public void testHashRangeInRangeRegular() {
        HashRange range = new HashRange("ABC", "DEF");
        assertTrue(range.isInRange("BBB"));
        assertFalse(range.isInRange("ABC"));
        assertFalse(range.isInRange("FFF"));
    }

    @Test
    public void testHashRangeInRangeRegularWrappedAround() {
        HashRange range = new HashRange("DEF", "ABC");
        assertTrue(range.isInRange("FFF"));
        assertTrue(range.isInRange("ABC"));
        assertFalse(range.isInRange("BBB"));
        assertFalse(range.isInRange("DEF"));
    }


    @Override
    public void tearDown() {
        new File("data").delete();
    }



    @Test
    public void testLruCache() {
        int cacheSize = 3;
        LruCachedStorage cache = new LruCachedStorage(cacheSize, "127.0.0.1", 50000);
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");

        // Check put
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key3"));

        // Check get
        assertEquals("value1", cache.get("key1").getValue());
        assertEquals("value2", cache.get("key2").getValue());
        assertEquals("value3", cache.get("key3").getValue());

        // Check update
        cache.put("key1", "value11");
        assertEquals("value11", cache.get("key1").getValue());

        // Check eviction: should evict key2 -> least recently used
        cache.put("key4", "value4");
        assertFalse(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key3"));
        assertTrue(cache.containsKey("key4"));

        // Check delete
        cache.delete("key4");
        assertFalse(cache.containsKey("key4"));
        assertFalse(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key3"));

        // Check load from disk
        assertEquals("value2", cache.get("key2").getValue());
    }


    @Test
    public void testFifoCache() {
        int cacheSize = 3;
        FifoCachedStorage cache = new FifoCachedStorage(cacheSize, "127.0.0.1", 50000);
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");

        // Check put
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key3"));

        // Check get
        assertEquals("value3", cache.get("key3").getValue());
        assertEquals("value2", cache.get("key2").getValue());
        assertEquals("value1", cache.get("key1").getValue());

        // Check update
        cache.put("key1", "value11");
        assertEquals("value11", cache.get("key1").getValue());

        // Check eviction: should evict key1 -> first element added to cachedStorage
        cache.put("key4", "value4");
        assertFalse(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key3"));
        assertTrue(cache.containsKey("key4"));

        // Check delete
        cache.delete("key4");
        assertFalse(cache.containsKey("key4"));
        assertFalse(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key3"));

        // Check load from disk
        assertEquals("value11", cache.get("key1").getValue());

        // Check eviction after delete: should evict key2 -> second element added to cachedStorage
        cache.put("key5", "value5");
        assertFalse(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key3"));
        assertTrue(cache.containsKey("key5"));
    }

    @Test
    public void testLfuCache() {
        int cacheSize = 3;
        LfuCachedStorage cache = new LfuCachedStorage(cacheSize, "127.0.0.1", 50000);
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");

        // Check put
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key3"));

        assertEquals("value3", cache.get("key3").getValue());
        assertEquals("value2", cache.get("key2").getValue());
        assertEquals("value1", cache.get("key1").getValue());

        // Check update
        cache.put("key1", "value11");
        assertEquals("value11", cache.get("key1").getValue());
        cache.put("key3", "value33");
        assertEquals("value33", cache.get("key3").getValue());

        // Check eviction: should evict key2 -> least accessed element
        cache.put("key4", "value4");
        assertFalse(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key3"));
        assertTrue(cache.containsKey("key4"));

        // Check delete
        cache.delete("key4");
        assertFalse(cache.containsKey("key4"));
        assertFalse(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key3"));

        // Check load from disk
        assertEquals("value2", cache.get("key2").getValue());

        // Increase frequency count by accessing key3 and key2 so that key1 becomes least frequent element
        for (int i = 0; i < 5; i ++) {
            cache.get("key3").getValue();
            cache.get("key2").getValue();
        }

        // Check eviction after delete: should evict key1
        cache.put("key5", "value5");
        assertFalse(cache.containsKey("key1"));
        assertTrue(cache.containsKey("key2"));
        assertTrue(cache.containsKey("key3"));
        assertTrue(cache.containsKey("key5"));
    }

    @Test
    public void testPersistence() {
        AbstractCachedStorage testCache = new AbstractCachedStorage("127.0.0.1", 50000) {
            @Override
            public KVMessage put(String key, String value) {
                logger.error("testPersistence: This is a persistence test, do not call CachedStorage.put()");
                fail("This is a persistence test, do not call CachedStorage.put()");
                return null;
            }

            @Override
            public KVMessage get(String key) {
                logger.error("testPersistence: This is a persistence test, do not call CachedStorage.get()");
                fail("This is a persistence test, do not call CachedStorage.get()");
                return null;
            }

            @Override
            public KVMessage delete(String key) {
                logger.error("testPersistence: This is a persistence test, do not call CachedStorage.delete()");
                fail("This is a persistence test, do not call CachedStorage.delete()");
                return null;
            }
        };

        try {
            assertNull(testCache.loadFromDisk("fileNotExist"));

            testCache.persistToDisk("key1", "value0");
            testCache.persistToDisk("key1", "value1");
            testCache.persistToDisk("key2", "value2");

            String actualValue1 = testCache.loadFromDisk("key1");
            String actualValue2 = testCache.loadFromDisk("key2");

            assertEquals("value1", actualValue1);
            assertEquals("value2", actualValue2);
            assertNull(testCache.loadFromDisk("keyNotExist"));

            testCache.deleteFromDisk("key2");
            assertNull(testCache.loadFromDisk("key2"));

            testCache.persistToDisk("alice\\,bob,\"", "value3");
            assertEquals("value3", testCache.loadFromDisk("alice\\,bob,\""));
        } catch (Exception e) {
            logger.error("testPersistence: Should not trigger IO Exception.");
            fail("Should not trigger IO Exception.");
        }
    }


    @Test
    public void testPutGet() {
        Store client = new Store("127.0.0.1", 50000);
        try {
            client.connect();
            Thread.sleep(1000);
        } catch (Exception e) {
        }



        KVMessage response = null;
        Exception ex = null;
        try {
            response = client.put("key", "value");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);

        response = null;
        ex = null;
        try {
            response = client.get("key");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
        assertEquals("value", response.getValue());
    }

    @Test
    public void testPutDeleteGet() {
        Store client = new Store("127.0.0.1", 50000);
        try {
            client.connect();
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        KVMessage response = null;
        Exception ex = null;
        try {
            response = client.put("key", "value");
            Thread.sleep(1000);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);

        response = null;
        ex = null;
        try {
            response = client.put("key", "null");
            Thread.sleep(1000);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS);

        response = null;
        ex = null;
        try {
            response = client.get("key");

        } catch (Exception e) {
            ex = e;
        }
        assertEquals(response.getStatus(), KVMessage.StatusType.GET_ERROR);
    }

    @Test
    public void testMultipleClients() {
        Store client1 = new Store("127.0.0.1", 50000);
        try {
            client1.connect();
        } catch (Exception e) {
            logger.error("testMultipleClients: client1 cannot connect.");
            fail();
        }

        Store client2 = new Store("127.0.0.1", 50000);
        try {
            client2.connect();
        } catch (Exception e) {
            logger.error("testMultipleClients: client2 cannot connect.");
            fail();
        }

        Store client3 = new Store("127.0.0.1", 50000);
        try {
            client3.connect();
        } catch (Exception e) {
            logger.error("testMultipleClients: client3 cannot connect.");
            fail();
        }

        KVMessage response = null;
        Exception ex = null;
        try {
            response = client1.put("key1", "value1");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);

        response = null;
        ex = null;
        try {
            response = client2.put("key2", "value2");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);

        response = null;
        ex = null;
        try {
            response = client3.put("key3", "value3");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);

        response = null;
        ex = null;
        try {
            response = client3.get("key1");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
        assertEquals("value1", response.getValue());

        response = null;
        ex = null;
        try {
            response = client2.get("key3");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
        assertEquals("value3", response.getValue());

        response = null;
        ex = null;
        try {
            response = client1.get("key2");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_SUCCESS || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
        assertEquals("value2", response.getValue());
    }

    @Test
    public void testPutFailFifo_shouldReturnErrorNotHoldLockNotModifyCache() {
        FifoCachedStorage cache = new FifoCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String persistToDisk(String key, String value) throws IOException {
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.put("key", "value");
        assertEquals(KVMessage.StatusType.PUT_ERROR, actualMessage.getStatus());
        assertFalse(cache.containsKey("key"));
        actualMessage = cache.put("key1", "value2");
        assertEquals(KVMessage.StatusType.PUT_ERROR, actualMessage.getStatus());
        assertFalse(cache.containsKey("key1"));
    }

    @Test
    public void testPutFailLru_shouldReturnErrorNotHoldLockNotModifyCache() {
        LruCachedStorage cache = new LruCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String persistToDisk(String key, String value) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.put("key", "value");
        assertEquals(KVMessage.StatusType.PUT_ERROR, actualMessage.getStatus());
        assertFalse(cache.containsKey("key"));
        actualMessage = cache.put("key1", "value2");
        assertEquals(KVMessage.StatusType.PUT_ERROR, actualMessage.getStatus());
        assertFalse(cache.containsKey("key1"));
    }

    @Test
    public void testPutFailLfu_shouldReturnErrorNotHoldLockNotModifyCache() {
        LfuCachedStorage cache = new LfuCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String persistToDisk(String key, String value) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.put("key", "value");
        assertEquals(KVMessage.StatusType.PUT_ERROR, actualMessage.getStatus());
        assertFalse(cache.containsKey("key"));
        actualMessage = cache.put("key1", "value2");
        assertEquals(KVMessage.StatusType.PUT_ERROR, actualMessage.getStatus());
        assertFalse(cache.containsKey("key1"));
    }

    @Test
    public void testGetFailFifo_shouldReturnErrorNotHoldLock() {
        FifoCachedStorage cache = new FifoCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String loadFromDisk(String key) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.get("key");
        assertEquals(KVMessage.StatusType.GET_ERROR, actualMessage.getStatus());
        actualMessage = cache.get("key1");
        assertEquals(KVMessage.StatusType.GET_ERROR, actualMessage.getStatus());
    }

    @Test
    public void testGetFailLru_shouldReturnErrorNotHoldLock() {
        LruCachedStorage cache = new LruCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String loadFromDisk(String key) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.get("key");
        assertEquals(KVMessage.StatusType.GET_ERROR, actualMessage.getStatus());
        actualMessage = cache.get("key1");
        assertEquals(KVMessage.StatusType.GET_ERROR, actualMessage.getStatus());
    }

    @Test
    public void testGetFailLfu_shouldReturnErrorNotHoldLock() {
        LfuCachedStorage cache = new LfuCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String loadFromDisk(String key) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.get("key");
        assertEquals(KVMessage.StatusType.GET_ERROR, actualMessage.getStatus());
        actualMessage = cache.get("key1");
        assertEquals(KVMessage.StatusType.GET_ERROR, actualMessage.getStatus());
    }

    @Test
    public void testDeleteFailFifo_shouldReturnErrorNotHoldLock() {
        FifoCachedStorage cache = new FifoCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String deleteFromDisk(String key) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.delete("key");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, actualMessage.getStatus());
        actualMessage = cache.delete("key1");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, actualMessage.getStatus());
    }

    @Test
    public void testDeleteFailLru_shouldReturnErrorNotHoldLock() {
        LruCachedStorage cache = new LruCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String deleteFromDisk(String key) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.delete("key");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, actualMessage.getStatus());
        actualMessage = cache.delete("key1");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, actualMessage.getStatus());
    }

    @Test
    public void testDeleteFailLfu_shouldReturnErrorNotHoldLock() {
        LfuCachedStorage cache = new LfuCachedStorage(1, "127.0.0.1", 50000) {
            @Override
            public String deleteFromDisk(String key) throws IOException{
                throw new IOException();
            }
        };

        KVMessage actualMessage = cache.delete("key");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, actualMessage.getStatus());
        actualMessage = cache.delete("key1");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, actualMessage.getStatus());
    }

}
