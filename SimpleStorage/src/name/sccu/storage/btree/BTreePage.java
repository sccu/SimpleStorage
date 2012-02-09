package name.sccu.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;

import name.sccu.storage.btree.BTreeHeader.StackItem;
import name.sccu.storage.btree.key.BTreeKey;


public interface BTreePage {
	
	public static final int NEXT_SEQ_POSITION = 4;
	
	public static class Factory {
		public static BTreePage create(byte[] bytes) {
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			if (bb.getInt(NEXT_SEQ_POSITION) == -1) {
				return new BTreeInternalNode(bytes);
			}
			else {
				return new BTreeLeafNode(bytes);
			}
		}
	}
	
	public static class BTreePageHolder {
		private BTreePage page;

		public BTreePage get() {
			return page;
		}

		public void set(BTreePage page) {
			this.page = page;
		}
	}

	int getPageNumber();

	void addKey(BTreeKey key, int rightPageNumber, int index);

	void setChild(int i, int child);

	int getChild(int i);

	BTreeKey getKey(int i);
	
	void addRecord(BTreeRecord record, int index);

	BTreeRecord getRecord(int index);
	
	void removeRecord(int index);
	
	void removeKey(int index);

	boolean isFull();

	boolean isLeaf();
	
	void copyNode(BTreePage targetPage, int from, int count);
	
	BTreeKey splitLeaf(BTreeRecord record, BTreePage rightPage, int index) throws IOException;
	
	BTreeKey splitNode(BTreeKey key, int newChild, BTreePage rightPage, int index) throws IOException;

	void merge(BTreePage sibling, BTreePage parent, StackItem item) throws IOException;

	int getKeyCount();

	void redistribute(BTreePage sibling, BTreePage parent, int index) throws IOException;

	void setKey(int index, BTreeKey key);

	byte[] toBytes();
}
