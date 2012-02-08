/**
 * 
 */
package sccu.storage.btree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
	public void testStringGetBytes() {
		String str = "abcde";
		byte[] b = str.getBytes();
		b[0] = 'c';
		System.out.println(str);
	}

}
