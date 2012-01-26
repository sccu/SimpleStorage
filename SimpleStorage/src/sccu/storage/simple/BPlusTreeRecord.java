package sccu.storage.simple;

import java.io.Serializable;


public class BPlusTreeRecord {

	public static class Key implements Serializable {
		private static final long serialVersionUID = 6875735879740205743L;
		private int key;
		public Key(int k) {
			this.key = k;
		}

		public static int getSize() {
			return 4;
		}

		public int getInt() {
			return key;
		}
		
		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			return other instanceof Key && this.key == ((Key)other).key;
		}

		public boolean lessThan(Key rhs) {
			return this.key < rhs.key;
		}
	}

	public static class Value {
		public static int getSize() {
			return 12;
		}
	}

	public BPlusTreeRecord() {
	}
	
	public BPlusTreeRecord(int key, String value) {
		this.key = new Key(key);
		this.value = value;
	}

	public static int getSize() {
		return Key.getSize() + Value.getSize();
	}

	private Key key;
	private String value;

	public void copyFrom(BPlusTreeRecord record) {
		this.key = record.key;
		this.value = record.value;
	}

	public Key getKey() {
		return key;
	}
	
	public String getValue() {
		return value;
	}

	BPlusTreeRecord deepCopy() {
		BPlusTreeRecord newRecord = new BPlusTreeRecord();
		newRecord.copyFrom(this);
		return newRecord;
	}
}
