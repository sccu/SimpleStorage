package name.sccu.storage.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;


public class BTreePageTest {

	@Test
	public void testByteBuffer() {
		byte[] buffer = new byte[5];
		buffer[3] = 4;
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.BIG_ENDIAN);
		assertEquals(4, bb.getInt());
		assertEquals(0, bb.get());
		
		int i = 0;
		assertFalse("i/2-1+i%2 == (i-1)/2 is false when i = 0", i/2-1+i%2 == (i-1)/2);
		for (i = 1; i < 100; i++) {
			assertTrue(i/2-1+i%2 == (i-1)/2);
		}
	}

}
