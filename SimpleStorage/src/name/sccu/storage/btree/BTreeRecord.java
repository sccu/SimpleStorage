package name.sccu.storage.btree;

import name.sccu.storage.btree.key.BTreeKey;



class BTreeRecord {
	
	public static class Key {
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
			return other instanceof Key && this.key == ((Key) other).key;
		}

		public boolean lessThan(Key rhs) {
			return this.key < rhs.key;
		}
	}

	public static class Value {
		public static int getSize() {
			return 8;
		}
	}

	public BTreeRecord() {
	}

	public BTreeRecord(int key, String value) {
		this.key = new BTreeKey(key);
		this.value = value;
	}

	public static int getSize() {
		return Key.getSize() + Value.getSize();
	}

	private BTreeKey key;
	private String value;

	public void copyFrom(BTreeRecord record) {
		this.key = record.key;
		this.value = record.value;
	}

	public BTreeKey getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	BTreeRecord deepCopy() {
		BTreeRecord newRecord = new BTreeRecord();
		newRecord.copyFrom(this);
		return newRecord;
	}
}
