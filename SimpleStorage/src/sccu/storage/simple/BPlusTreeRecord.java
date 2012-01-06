package sccu.storage.simple;

public class BPlusTreeRecord {

	public static class Key {
		private int key;
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
		// TODO Auto-generated method stub
		return key;
	}

}
