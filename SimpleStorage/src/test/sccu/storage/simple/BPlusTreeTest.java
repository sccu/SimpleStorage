package test.sccu.storage.simple;

import static org.junit.Assert.*;

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

	private static final int PAGE_SIZE = 128;
	private static final int SIZE = 1000;
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
			System.out.println("Deleting key " + i);
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
			System.out.println("Deleting reversely. Key :" + i);
			assertTrue(m_tree.deleteRecord(new Key(i)));
		}
		for (int i = 0; i < SIZE; i++) {
			BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
			assertFalse("Retrieve KEY:"+i, m_tree.retrieveRecord(new Key(i), record));
		}
	}
	
	@Test
	public void testInsertRandomly() throws IOException {
		for (long seed = 0; seed < 10; seed++) {
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
				BPlusTreeRecord record = new BPlusTreeRecord(i, Integer.toString(i));
				assertTrue(m_tree.retrieveRecord(new Key(i), record));
				assertEquals(Integer.toString(i), record.getValue().trim());
			}
			BPlusTreeRecord record = new BPlusTreeRecord(-1, Integer.toString(-1));
			assertFalse(m_tree.retrieveRecord(new Key(-1), record));
			assertFalse(m_tree.retrieveRecord(new Key(list.size()), record));
		}
	}
	
}
