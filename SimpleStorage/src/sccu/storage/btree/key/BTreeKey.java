package sccu.storage.btree.key;

public interface BTreeKey<T> extends Comparable<T> {
	public byte[] toBytes();
}
