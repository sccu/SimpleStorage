package test.sccu.storage.btree.key;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import sccu.storage.btree.key.BTreeKey;
import sccu.storage.btree.key.IntegerKey;
import sccu.storage.btree.key.MultipleColumnKey;
import sccu.storage.btree.key.StringKey;

public class MutipleColumnKeyTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		BTreeKey<?>[] array = new BTreeKey<?>[2];
		array[0] = new StringKey("abcde", 5); 
		array[1] = new IntegerKey(4);
		MultipleColumnKey key = new MultipleColumnKey(array);
		
		array[0] = new StringKey("abcde", 5);
		MultipleColumnKey equal0 = new MultipleColumnKey(array);
		assertTrue(equal0.compareTo(key) == 0);
		
		array[0] = new StringKey("abcdefg", 5);
		MultipleColumnKey equal1 = new MultipleColumnKey(array);
		assertTrue(equal1.compareTo(key) == 0);
		
		array[1] = new IntegerKey(2);
		MultipleColumnKey less0 = new MultipleColumnKey(array);
		assertTrue(less0.compareTo(key) < 0);
		assertTrue(key.compareTo(less0) > 0);
		
		array[0] = new StringKey("abcdd", 8);
		MultipleColumnKey less1 = new MultipleColumnKey(array);
		assertTrue(less1.compareTo(key) < 0);
		assertTrue(key.compareTo(less1) > 0);
		
		array[0] = new StringKey("abcdf", 8);
		MultipleColumnKey greater1 = new MultipleColumnKey(array);
		assertTrue(key.compareTo(greater1) < 0);
		assertTrue(greater1.compareTo(key) > 0);
		
		array = new BTreeKey<?>[1];
		array[0] = new StringKey("abcde", 5); 
		MultipleColumnKey less2 = new MultipleColumnKey(array);
		assertTrue(less2.compareTo(key) < 0);
		assertTrue(key.compareTo(less2) > 0);
	}

}
