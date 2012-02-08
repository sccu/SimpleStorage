package sccu.storage.btree.key;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import sccu.storage.btree.key.BTreeDataType;
import sccu.storage.btree.key.IntegerType;
import sccu.storage.btree.key.BTreeKey;
import sccu.storage.btree.key.StringType;

public class BTreeKeyTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		BTreeDataType<?>[] array = new BTreeDataType<?>[2];
		array[0] = new StringType("abcde", 5); 
		array[1] = new IntegerType(4);
		BTreeKey key = new BTreeKey(array);
		
		array[0] = new StringType("abcde", 5);
		BTreeKey equal0 = new BTreeKey(array);
		assertTrue(equal0.compareTo(key) == 0);
		assertArrayEquals(key.toBytes(), equal0.toBytes());
		
		array[0] = new StringType("abcdefg", 5);
		BTreeKey equal1 = new BTreeKey(array);
		assertTrue(equal1.compareTo(key) == 0);
		assertArrayEquals(key.toBytes(), equal1.toBytes());
		
		array[1] = new IntegerType(2);
		BTreeKey less0 = new BTreeKey(array);
		assertTrue(less0.compareTo(key) < 0);
		assertTrue(key.compareTo(less0) > 0);
		
		array[0] = new StringType("abcdd", 8);
		BTreeKey less1 = new BTreeKey(array);
		assertTrue(less1.compareTo(key) < 0);
		assertTrue(key.compareTo(less1) > 0);
		
		array[0] = new StringType("abcdf", 8);
		BTreeKey greater1 = new BTreeKey(array);
		assertTrue(key.compareTo(greater1) < 0);
		assertTrue(greater1.compareTo(key) > 0);
		
		array = new BTreeDataType<?>[1];
		array[0] = new StringType("abcde", 5); 
		BTreeKey less2 = new BTreeKey(array);
		assertTrue(less2.compareTo(key) < 0);
		assertTrue(key.compareTo(less2) > 0);
	}

}
