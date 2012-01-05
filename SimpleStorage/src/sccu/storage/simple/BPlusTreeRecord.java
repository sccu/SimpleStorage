package sccu.storage.simple;

public class BPlusTreeRecord {

	public static class Key {
		public static int getSize() {
			return 4;
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

	private int key;
	private String value;

	public void copy(BPlusTreeRecord record) {
		this.key = record.key;
		this.value = record.value;
	}

}
