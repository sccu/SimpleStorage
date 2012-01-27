package sccu.storage.btree.key;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultipleColumnKey implements BTreeKey<MultipleColumnKey> {

	private final BTreeKey<?>[] columns;

	public MultipleColumnKey(BTreeKey<?>[] columns) {
		this.columns = Arrays.copyOf(columns, columns.length);
	}

	@Override
	public int compareTo(MultipleColumnKey other) {
		for (int i = 0; i < this.columns.length; i++) {
			if (other.columns.length <= i) {
				return 1;
			}

			@SuppressWarnings({ "unchecked", "rawtypes" })
			int result = ((BTreeKey) this.columns[i]).compareTo((BTreeKey) other.columns[i]);
			if (result != 0) {
				return result;
			}
		}
		
		return this.columns.length - other.columns.length;
	}

	@Override
	public byte[] toBytes() {
		List<byte[]> list = new ArrayList<byte[]>(this.columns.length);
		int totalSize = 0;
		for (BTreeKey<?> column: this.columns) {
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
