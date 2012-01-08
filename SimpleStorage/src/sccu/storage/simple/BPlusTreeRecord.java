package sccu.storage.simple;

public class BPlusTreeRecord {

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
	}

	public static class Value {
		public static int getSize() {
			return 12;
		}
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

	BPlusTreeRecord deepCopy() {
		BPlusTreeRecord newRecord = new BPlusTreeRecord();
		newRecord.copyFrom(this);
		return newRecord;
	}
}
