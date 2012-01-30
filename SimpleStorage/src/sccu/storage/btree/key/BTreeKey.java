package sccu.storage.btree.key;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BTreeKey {

	private final BTreeDataType<?>[] columns;

	public BTreeKey(BTreeDataType<?>[] columns) {
		this.columns = Arrays.copyOf(columns, columns.length);
	}
	
	public BTreeKey(int value) {
		this.columns = new BTreeDataType<?>[1];
		this.columns[0] = new IntegerType(value);
	}

	public boolean lessThan(BTreeKey other) {
		return this.compareTo(other) < 0;
	}

	public int compareTo(BTreeKey other) {
		for (int i = 0; i < this.columns.length; i++) {
			if (other.columns.length <= i) {
				return 1;
			}

			@SuppressWarnings({ "unchecked", "rawtypes" })
			int result = ((BTreeDataType) this.columns[i]).compareTo((BTreeDataType) other.columns[i]);
			if (result != 0) {
				return result;
			}
		}
		
		return this.columns.length - other.columns.length;
	}
	
	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other instanceof BTreeKey) {
			return this.compareTo((BTreeKey)other) == 0;
		}
		return false;
	}

	public byte[] toBytes() {
		List<byte[]> list = new ArrayList<byte[]>(this.columns.length);
		int totalSize = 0;
		for (BTreeDataType<?> column: this.columns) {
			byte[] bytes = column.toBytes();
			totalSize += bytes.length;
			list.add(bytes);
		}
		
		byte[] results = new byte[totalSize];
		ByteBuffer bb = ByteBuffer.wrap(results);
		for (byte[] bytes : list) {
			bb.put(bytes);
		}
		
		return results;
	}

}
