package name.sccu.storage.btree.key;

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

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other instanceof IntegerType) {
			return this.compareTo((IntegerType)other) == 0;
		}
		return false;
	}

}