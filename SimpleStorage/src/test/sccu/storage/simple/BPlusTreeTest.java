package test.sccu.storage.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import sccu.storage.simple.BPlusTree;
import sccu.storage.simple.BPlusTreeRecord;
import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTreeTest {

	private static final int PAGE_SIZE = 256;
	private static final int SIZE = 10000;
	private BPlusTree m_tree;

	@Before
	public void setUp() throws Exception {
		m_tree = new BPlusTree();
		m_tree.initBTree("./data/test.btree", PAGE_SIZE, false);
	}

	@After
	public void tearDown() throws Exception {
		m_tree.closeBTree();
	}

	@Test
	public void testInsert() throws IOException {
		//m_tree.closeBTree();
		m_tree = new BPlusTree();
		m_tree.initBTree("./data/test.btree", PAGE_SIZE, true);
		
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertTrue(m_tree.insertRecord(record));
		}
		
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertTrue(m_tree.retrieveRecord(new Key(i), record));
			assertEquals(Integer.toString(i), record.getValue().trim());
		}
		BPlusTreeRecord record = new BPlusTreeRecord(-1, Integer.toString(-1));
		assertFalse(m_tree.retrieveRecord(new Key(-1), record));
		assertFalse(m_tree.retrieveRecord(new Key(SIZE), record));
	}

	@Test
	public void testLoad() throws IOException {
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertTrue(m_tree.retrieveRecord(new Key(i), record));
			assertEquals(Integer.toString(i), record.getValue().trim());
		}
		BPlusTreeRecord record = new BPlusTreeRecord(-1, Integer.toString(-1));
		assertFalse(m_tree.retrieveRecord(new Key(-1), record));
		assertFalse(m_tree.retrieveRecord(new Key(SIZE), record));
	}

	@Test
	public void testDelete() throws IOException {
		for (int i = 0; i < SIZE; i++) {
			assertTrue("Delete KEY:"+i, m_tree.deleteRecord(new Key(i)));
		}
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertFalse("Retrieve KEY:"+i, m_tree.retrieveRecord(new Key(i), record));
		}
	}
	
	@Test
	public void testInsertReversely() throws IOException {
		m_tree.closeBTree();
		m_tree = new BPlusTree();
		m_tree.initBTree("./data/test.btree", PAGE_SIZE, true);
		
		for (int i = SIZE-1; i >= 0; i--) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertTrue(m_tree.insertRecord(record));
		}
		
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertTrue(m_tree.retrieveRecord(new Key(i), record));
			assertEquals(Integer.toString(i), record.getValue().trim());
		}
		BPlusTreeRecord record = new BPlusTreeRecord(-1, Integer.toString(-1));
		assertFalse(m_tree.retrieveRecord(new Key(-1), record));
		assertFalse(m_tree.retrieveRecord(new Key(SIZE), record));
	}
	
	@Test
	public void testLoadAfterInsertReversely() throws IOException {
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertTrue(m_tree.retrieveRecord(new Key(i), record));
			assertEquals(Integer.toString(i), record.getValue().trim());
		}
		BPlusTreeRecord record = new BPlusTreeRecord(-1, Integer.toString(-1));
		assertFalse(m_tree.retrieveRecord(new Key(-1), record));
		assertFalse(m_tree.retrieveRecord(new Key(SIZE), record));
	}

	@Test
	public void testDeleteReversely() throws IOException {
		for (int i = SIZE-1; i >= 0; i--) {
			assertTrue(m_tree.deleteRecord(new Key(i)));
		}
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertFalse("Retrieve KEY:"+i, m_tree.retrieveRecord(new Key(i), record));
		}
	}
	
	@Test
	public void testInsertRandomly() throws IOException {
		for (long seed = 10; seed < 20; seed++) {
			m_tree.closeBTree();
			m_tree = new BPlusTree();
			m_tree.initBTree("./data/test.btree", PAGE_SIZE, true);
			
			System.out.println("Seed:" + seed);
			Random rand = new Random(seed);
			
			ArrayList<Integer> list = new ArrayList<Integer>();
			for (int i = 0; i < SIZE; i++) {
				list.add(new Integer(i));
			}
			Collections.shuffle(list, rand);
			
			for (Integer i : list) {
				BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
				assertTrue(m_tree.insertRecord(record));
			}
			
			for (Integer i : list) {
				BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(-2));
				assertTrue("i: " + i, m_tree.retrieveRecord(new Key(i), record));
				assertEquals("i: " + i, Integer.toString(i), record.getValue().trim());
			}
			BPlusTreeRecord record = new BPlusTreeRecord(-1, Integer.toString(-1));
			assertFalse(m_tree.retrieveRecord(new Key(-1), record));
			assertFalse(m_tree.retrieveRecord(new Key(list.size()), record));
		}
	}
	
	@Test
	public void testRandomly() throws IOException {
		m_tree.closeBTree();
		m_tree = new BPlusTree();
		m_tree.initBTree("./data/test.btree", PAGE_SIZE, true);
		
		boolean[] contains = new boolean[SIZE];
		
		for (long seed = 10; seed < 20; seed++) {
			System.out.println("Seed:" + seed);
			Random randomKey = new Random(seed);
			Random randomOperation = new Random(seed);
			
			BPlusTreeRecord record = new BPlusTreeRecord(0, "0");
			for (int i = 0; i < SIZE * 10; i++) {
				int key = randomKey.nextInt(SIZE);
				switch (randomOperation.nextInt(3)) {
				case 0:	// insert
					assertEquals("seed:" + seed + ", iter:"+i + ", key:" + key,
							!contains[key], m_tree.insertRecord(new BPlusTreeRecord(key, String.valueOf(key))));
					contains[key] = true;
					
					break;
				case 1:	// delete
					assertEquals("seed:" + seed + ", iter:"+i + ", key:" + key,
							contains[key], m_tree.deleteRecord(new Key(key)));
					contains[key] = false;
					
					break;
				case 2:	// retrieve 
					assertEquals(contains[key], m_tree.retrieveRecord(new Key(key), record));
					if (contains[key]) {
						assertEquals("seed:" + seed + ", iter:"+i + ", key:" + key, 
							Integer.toString(key), record.getValue().trim());
					}
					break;
				default:
				
				}
			}
		}
		BPlusTreeRecord record = new BPlusTreeRecord(0, "0");
		for (int j = 0; j < SIZE; j++) {
			assertEquals(contains[j], m_tree.retrieveRecord(new Key(j), record));
			if (contains[j]) {
				assertEquals(Integer.toString(j), record.getValue().trim());
			}
		}
	}
	
}