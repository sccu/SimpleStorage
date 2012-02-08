package name.sccu.storage.btree.key;

import java.util.Arrays;

public class StringType implements BTreeDataType<StringType> {

	private final String value;
	private final int limit;

	public StringType(String val, int limit) {
		if (val.length() > limit) {
			this.value = val.substring(0, limit);
		}
		else {
			this.value = val;
		}
		this.limit = limit;
	}

	@Override
	public int compareTo(StringType other) {
		return this.value.compareTo(other.value);
	}

	@Override
	public byte[] toBytes() {
		return Arrays.copyOf(this.value.getBytes(), limit);
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other instanceof StringType) {
			return this.compareTo((StringType)other) == 0;
		}
		return false;
	}

}
