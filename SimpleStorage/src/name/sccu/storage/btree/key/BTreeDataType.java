package name.sccu.storage.btree.key;

public interface BTreeDataType<T> extends Comparable<T> {
	public byte[] toBytes();
}
