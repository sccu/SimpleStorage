/**
 * 
 */
package test.sccu.storage.btree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author sccu
 *
 */
public class BufferManagerTest {

	public static class Array implements Serializable {
		private static final long serialVersionUID = 1026259799097567472L;
		private byte[] a = new byte[32];
	}
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
	public void testArray() throws IOException {
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(os);
		
		for (int i = 0; i < 1000; i++) {
			oos.writeObject(new Array());
		}
		byte[] ba = os.toByteArray();
		System.out.println("Size:" + ba.length);
		
	}
	
	@Test
	public void testStringGetBytes() {
		String str = "abcde";
		byte[] b = str.getBytes();
		b[0] = 'c';
		System.out.println(str);
	}

}
