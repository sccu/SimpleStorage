package sccu.storage.btree.key;

import java.nio.ByteBuffer;

public class IntegerType implements BTreeDataType<IntegerType> {

	private final int value;

	public IntegerType(int val) {
		this.value = val;
	}

	@Override
	public int compareTo(IntegerType other) {
		return this.value - other.value;
	}

	@Override
	public byte[] toBytes() {
		byte[] bytes = new byte[4];
		ByteBuffer.wrap(bytes).putInt(this.value);
		return bytes;
	}

}