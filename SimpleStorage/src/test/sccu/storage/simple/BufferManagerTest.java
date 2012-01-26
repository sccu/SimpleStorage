/**
 * 
 */
package test.sccu.storage.simple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import sccu.storage.simple.BPlusTreeRecord;

/**
 * @author sccu
 *
 */
public class BufferManagerTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(os);
		
//		oos.writeObject(new BPlusTreeRecord.Key(1));
//		oos.writeObject(new BPlusTreeRecord.Key(2));
//		oos.writeObject(new BPlusTreeRecord.Key(3));
//		oos.writeObject(new BPlusTreeRecord.Key(5));
//		oos.writeObject(new BPlusTreeRecord.Key(1203981234));
		oos.writeObject(new Integer(1));
		oos.writeObject(new Integer(2));
		byte[] ba = os.toByteArray();
		//System.out.print(ba.length);
		
		for (byte b : ba) {
			System.out.println(b);
		}
		
		System.out.println("Size:" + ba.length);
	}

}
