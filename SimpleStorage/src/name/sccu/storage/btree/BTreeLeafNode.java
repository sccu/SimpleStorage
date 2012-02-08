package name.sccu.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import name.sccu.storage.btree.BTreeHeader.StackItem;
import name.sccu.storage.btree.key.BTreeKey;


public class BTreeLeafNode implements BTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;
	private ArrayList<BTreeRecord> records = new ArrayList<BTreeRecord>();

	public BTreeLeafNode() throws IOException {
		this.pageNumber = newPageNumber();
		this.nextPageNumber = 0;
		this.keyCount = 0;
	}

	public BTreeLeafNode(int pageNumber) {
		this.pageNumber = pageNumber;
		this.nextPageNumber = 0;
		this.keyCount = 0;
	}

	BTreeLeafNode(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		this.pageNumber = bb.getInt();
		this.nextPageNumber = bb.getInt();
		this.keyCount = bb.getInt();
		for (int i = 0; i < this.keyCount; i++) {
			int key = bb.getInt();
			byte[] value = new byte[BTreeRecord.Value.getSize()];
			bb.get(value);
			this.records.add(new BTreeRecord(key, new String(value)));
		}
	}

	private static int newPageNumber() throws IOException {
		return BufferManager.getInstance().newPageNumber();
	}
	
	public void writeBTreePage() throws IOException {
		//this.validate();
		BufferManager.getInstance().writePage(pageNumber, this.toBytes());
	}

	public byte[] toBytes() {
		byte[] buffer = new byte[BufferManager.getInstance().getPageSize()];
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		
		bb.putInt(this.pageNumber);
		bb.putInt(this.nextPageNumber);
		bb.putInt(this.keyCount);
		for (int i = 0; i < this.keyCount; i++) {
			bb.put(this.records.get(i).getKey().toBytes());
			byte[] value = Arrays.copyOf(this.records.get(i).getValue().getBytes(), BTreeRecord.Value.getSize());
			bb.put(value);
		}
	
		return buffer;
	}

	/*
	private void validate() throws IOException {
		if (this.isLeaf()) {
			if (this.keyCount != this.records.size()) {
				throw new IOException("Invalid Page: " + this.getPageNumber() + 
						" KeyCount: " + this.keyCount +
						" RecordSize: " + this.records.size());
			}
			for (int i = 0; i < this.keyCount - 1; i++) {
				if (!this.getRecord(i).getKey().lessThan(this.getRecord(i+1).getKey())) {
					throw new IOException("Invalid Page: " + this.getPageNumber() + " Index: " + i);
				}
			}
		}
		else {
			for (int i = 0; i < this.keyCount - 1; i++) {
				if (!this.getKey(i).lessThan(this.getKey(i+1))) {
					throw new IOException("Invalid Page: " + this.getPageNumber() + " Index: " + i);
				}
			}
		}
	}
	*/

	public int getPageNumber() {
		return pageNumber;
	}

	public void addKey(BTreeKey key, int rightPageNumber, int index) {
		for (int i = this.keyCount; i > index; i--) {
			setKey(i, getKey(i-1));
			setChild(i+1, getChild(i));
		}
		setKey(index, key);
		setChild(index+1, rightPageNumber);
		this.keyCount++;
	}

	public void setChild(int i, int child) {
		throw new UnsupportedOperationException();
	}

	public int getChild(int i) {
		throw new UnsupportedOperationException();
	}

	public void setKey(int i, BTreeKey key) {
		throw new UnsupportedOperationException();
	}

	public BTreeKey getKey(int i) {
		throw new UnsupportedOperationException();
	}

	public void addRecord(BTreeRecord record, int index) {
		this.records.add(index, record.deepCopy());
		this.keyCount++;
	}

	public BTreeRecord getRecord(int index) {
		return this.records.get(index);
	}
	
	public void removeRecord(int index) {
		this.records.remove(index);
		this.keyCount--;
	}
	
	public void removeKey(int index) {
		for (int i = index; i < this.keyCount-1; i++) {
			this.setKey(i, this.getKey(i+1));
			this.setChild(i+1, this.getChild(i+2));
		}
		this.keyCount--;
	}

	public boolean isFull() {
		return this.keyCount == BTreeHeader.getMaxRecord();
	}

	public boolean isLeaf() {
		return true;
	}
	
	public void copyNode(BTreePage targetPage, int from, int count) {
		BTreeLeafNode target = (BTreeLeafNode)targetPage;
		target.records.clear();
		for (int i = 0; i < count; i++) {
			target.records.add(this.getRecord(i+from).deepCopy());
		}
		target.keyCount = count;
	}
	
	public BTreeKey splitLeaf(BTreeRecord record, BTreePage rightPage, int index) throws IOException {
		BTreeLeafNode tempPage = new BTreeLeafNode(-2);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addRecord(record, index);
		
		//int midIndex = tempPage.keyCount/2 - 1 + tempPage.keyCount%2;
		int midIndex = (tempPage.keyCount-1) / 2;
		BTreeKey midKey = tempPage.getRecord(midIndex).getKey();
		
		BTreeLeafNode right = (BTreeLeafNode)rightPage;
		right.nextPageNumber = this.nextPageNumber;
		this.nextPageNumber = right.pageNumber;
		
		tempPage.copyNode(this, 0, midIndex+1);
		this.writeBTreePage();
		
		tempPage.copyNode(rightPage, midIndex+1, tempPage.keyCount-midIndex-1);
		BufferManager.getInstance().writePage(rightPage);
		
		return midKey;
	}

	public BTreeKey splitNode(BTreeKey key, int newChild, BTreePage rightPage, int index) throws IOException {
		throw new UnsupportedOperationException();
	}

	public int getKeyCount() {
		return this.keyCount;
	}

	public void freeBTreePage() throws IOException {
		BufferManager.getInstance().freePage(this.getPageNumber());
	}

	public void merge(BTreePage siblingPage, BTreePage parent,
			StackItem item) throws IOException {
		BTreeLeafNode child = this;
		if (item.index == parent.getKeyCount()) {
			siblingPage = child;
			item.index--;
			child = (BTreeLeafNode) BufferManager.getBTreePage(parent.getChild(item.index));
		}
		else {
			siblingPage = BufferManager.getBTreePage(parent.getChild(item.index+1));
		}
		
		BTreeLeafNode sibling = (BTreeLeafNode)siblingPage;
		for (int i = 0; i < sibling.keyCount; i++) {
			child.appendRecord(sibling.getRecord(i));
		}
		child.nextPageNumber = sibling.nextPageNumber;
		child.writeBTreePage();
		siblingPage.freeBTreePage();
	}

	private void appendRecord(BTreeRecord record) {
		this.records.add(record.deepCopy());
		this.keyCount++;
	}
	
	public void redistribute(BTreePage siblingPage,
			BTreePage parent, int index) throws IOException {
		BTreeLeafNode child = this;
		BTreeLeafNode sibling = (BTreeLeafNode)siblingPage;
		
		int moveCount = (sibling.getKeyCount() - child.getKeyCount()) / 2;
		
		if (child.getRecord(0).getKey().lessThan(sibling.getRecord(0).getKey())) {
			child.insertRecords(child.getKeyCount(), sibling.removeRecords(0, moveCount));
			parent.setKey(index, child.getRecord(child.getKeyCount()-1).getKey());
		}
		else {
			List<BTreeRecord> records = sibling.removeRecords(
					sibling.getKeyCount()-moveCount, sibling.getKeyCount());
			child.insertRecords(0, records);
			parent.setKey(index, sibling.getRecord(sibling.getKeyCount()-1).getKey());
		}
		
		child.writeBTreePage();
		sibling.writeBTreePage();
	}

	private void insertRecords(int index, List<BTreeRecord> records) {
		this.records.addAll(index, records);
		this.keyCount = this.records.size();
	}

	private List<BTreeRecord> removeRecords(int fromIndex, int toIndex) {
		List<BTreeRecord> results = this.records.subList(fromIndex, toIndex);
		
		ArrayList<BTreeRecord> newRecords = new ArrayList<BTreeRecord>();
		newRecords.addAll(this.records.subList(0, fromIndex));
		newRecords.addAll(this.records.subList(toIndex, this.records.size()));
		this.records = newRecords;
		this.keyCount = this.records.size();
		
		return results;
	}

}
