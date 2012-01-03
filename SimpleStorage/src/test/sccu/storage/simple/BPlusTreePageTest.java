package test.sccu.storage.simple;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;


public class BPlusTreePageTest {

	@Test
	public void testByteBuffer() {
		byte[] buffer = new byte[5];
		buffer[3] = 4;
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.BIG_ENDIAN);
		assertEquals(4, bb.getInt());
		assertEquals(0, bb.get());
	}

}
