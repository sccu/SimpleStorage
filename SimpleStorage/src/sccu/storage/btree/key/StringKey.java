package sccu.storage.btree.key;

import java.util.Arrays;

public class StringKey implements BTreeKey<StringKey> {

	private final String value;
	private final int limit;

	public StringKey(String val, int limit) {
		if (val.length() > limit) {
			this.value = val.substring(0, limit);
		}
		else {
			this.value = val;
		}
		this.limit = limit;
	}

	@Override
	public int compareTo(StringKey other) {
		return this.value.compareTo(other.value);
	}

	@Override
	public byte[] toBytes() {
		return Arrays.copyOf(this.value.getBytes(), limit);
	}

}
