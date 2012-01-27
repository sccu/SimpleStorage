package sccu.storage.btree.key;

import java.nio.ByteBuffer;

public class IntegerKey implements BTreeKey<IntegerKey> {

	private final int value;

	public IntegerKey(int val) {
		this.value = val;
	}

	@Override
	public int compareTo(IntegerKey other) {
		return this.value - other.value;
	}

	@Override
	public byte[] toBytes() {
		byte[] bytes = new byte[4];
		ByteBuffer.wrap(bytes).putInt(this.value);
		return bytes;
	}

}