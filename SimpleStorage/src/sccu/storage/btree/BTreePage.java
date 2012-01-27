package sccu.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sccu.storage.btree.BTreeHeader.StackItem;
import sccu.storage.btree.BTreeRecord.Key;

public class BTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;
	private int[] data = null;
	private ArrayList<BTreeRecord> records = new ArrayList<BTreeRecord>();

	public BTreePage(boolean leaf) throws IOException {
		this.pageNumber = newPageNumber();
		if (leaf) {
			this.nextPageNumber = 0;
		}
		else {
			this.nextPageNumber = -1;
		}
		
		this.keyCount = 0;
		if (!leaf) {
			this.data = new int[BufferManager.getInstance().getPageSize()/4];
		}
	}

	public BTreePage(int pageNumber, boolean leaf) {
		this.pageNumber = pageNumber;
		if (leaf) {
			this.nextPageNumber = 0;
		}
		else {
			this.nextPageNumber = -1;
		}
		
		this.keyCount = 0;
		if (!leaf) {
			this.data = new int[BufferManager.getInstance().getPageSize()/4];
		}
	}

	public BTreePage() {
	}

	private static int newPageNumber() throws IOException {
		return BufferManager.getInstance().newPageNumber();
	}
	
	public void readBTreePage(int pageNo) throws IOException {
		this.records.clear();
		
		byte[] buffer = BufferManager.getInstance().readPage(pageNo);
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		this.pageNumber = bb.getInt();
		this.nextPageNumber = bb.getInt();
		this.keyCount = bb.getInt();
		if (this.isLeaf()) {
			for (int i = 0; i < this.keyCount; i++) {
				int key = bb.getInt();
				byte[] value = new byte[BTreeRecord.Value.getSize()];
				bb.get(value);
				this.records.add(new BTreeRecord(key, new String(value)));
			}
		}
		else {
			// TODO: array를 통째로 복사. serializable 구현
			this.data = new int[BufferManager.getInstance().getPageSize()/4];
			this.setChild(0, bb.getInt());
			for (int i = 0; i < this.keyCount; i++) {
				this.setKey(i, new Key(bb.getInt()));
				this.setChild(i+1, bb.getInt());
			}
		}
	}

	public void writeBTreePage() throws IOException {
		this.validate();
		BufferManager.getInstance().writePage(pageNumber, this.toBytes());
	}

	private byte[] toBytes() {
		byte[] buffer = new byte[BufferManager.getInstance().getPageSize()];
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		
		bb.putInt(this.pageNumber);
		bb.putInt(this.nextPageNumber);
		bb.putInt(this.keyCount);
		if (this.isLeaf()) {
			for (int i = 0; i < this.keyCount; i++) {
				bb.putInt(this.records.get(i).getKey().getInt());
				byte[] value = Arrays.copyOf(this.records.get(i).getValue().getBytes(), BTreeRecord.Value.getSize());
				bb.put(value);
			}
		}
		else {
			// TODO: array를 통째로 복사. serializable 구현
			bb.putInt(this.getChild(0));
			for (int i = 0; i < this.keyCount; i++) {
				bb.putInt(this.getKey(i).getInt());
				bb.putInt(this.getChild(i+1));
			}
		}
		
		return buffer;
	}

	private void validate() throws IOException {
		if (this.isLeaf()) {
			if (this.keyCount != this.records.size()) {
				throw new IOException("Invalid Page: " + this.getPageNumber() + 
						" KeyCount: " + this.keyCount +
						" RecordSize: " + this.records.size());
			}
			for (int i = 0; i < this.keyCount - 1; i++) {
				if (this.getRecord(i).getKey().getInt() >= this.getRecord(i+1).getKey().getInt()) {
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

	public int getPageNumber() {
		return pageNumber;
	}

	public int getNextPageNumber() {
		return this.nextPageNumber;
	}
	
	public void addKey(Key key, int rightPageNumber, int index) {
		for (int i = this.keyCount; i > index; i--) {
			setKey(i, getKey(i-1));
			setChild(i+1, getChild(i));
		}
		setKey(index, key);
		setChild(index+1, rightPageNumber);
		this.keyCount++;
	}

	void setChild(int i, int child) {
		data[i*2] = child;
	}

	int getChild(int i) {
		return data[i*2];
	}

	private void setKey(int i, Key key) {
		data[i*2+1] = key.getInt();
	}

	Key getKey(int i) {
		return new Key(data[i*2+1]);
	}

	public void addRecord(BTreeRecord record, int index) {
		this.records.add(index, record.deepCopy());
		this.keyCount++;
	}

	BTreeRecord getRecord(int index) {
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

	boolean isFull() {
		if (this.isLeaf()) {
			return this.keyCount == BTreeHeader.getMaxRecord();
		}
		else {
			return this.keyCount == BTreeHeader.getOrder()-1;
		}
	}

	boolean isLeaf() {
		return this.nextPageNumber != -1;
	}
	
	public void copyNode(BTreePage targetPage, int from, int count) {
		targetPage.keyCount = 0;
		if (this.isLeaf()) {
			targetPage.records.clear();
			for (int i = 0; i < count; i++) {
				//TODO delete if statement.
				if (i < targetPage.records.size()) {
					targetPage.getRecord(i).copyFrom(this.getRecord(i+from));
				}
				else {
					targetPage.records.add(this.getRecord(i+from).deepCopy());
				}
				targetPage.keyCount++;
			}
		}
		else {
			for (int i = 0; i < count; i++) {
				targetPage.setChild(i, this.getChild(i+from));
				targetPage.setKey(i, this.getKey(i+from));
				targetPage.keyCount++;
			}
			targetPage.setChild(count, this.getChild(count+from));
		}
	}
	
	public Key splitLeaf(BTreeRecord record, BTreePage rightPage, int index) throws IOException {
		BTreePage tempPage = new BTreePage(-2, true);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addRecord(record, index);
		
		//int midIndex = tempPage.keyCount/2 - 1 + tempPage.keyCount%2;
		int midIndex = (tempPage.keyCount-1) / 2;
		Key midKey = tempPage.getRecord(midIndex).getKey();
		
		rightPage.nextPageNumber = this.nextPageNumber;
		this.nextPageNumber = rightPage.pageNumber;
		
		tempPage.copyNode(this, 0, midIndex+1);
		this.writeBTreePage();
		
		tempPage.copyNode(rightPage, midIndex+1, tempPage.keyCount-midIndex-1);
		rightPage.writeBTreePage();
		
		return midKey;
	}

	public Key splitNode(Key key, int newChild, BTreePage rightPage, int index) throws IOException {
		BTreePage tempPage = new BTreePage(-2, false);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addKey(key, newChild, index);
		
		int midIndex = tempPage.keyCount / 2;
		Key midKey = tempPage.getKey(midIndex);
		
		tempPage.copyNode(this, 0, midIndex);
		this.writeBTreePage();
		
		tempPage.copyNode(rightPage, midIndex+1, tempPage.keyCount-midIndex-1);
		rightPage.writeBTreePage();
		
		return midKey;
	}

	public void mergeNode(BTreePage sibling,
			BTreePage parent, StackItem item) throws IOException {
		BTreePage child = this;
		if (item.index == parent.getKeyCount()) {
			BTreePage tempPage = sibling;
			sibling = child;
			child = tempPage;
			item.index--;
			child.readBTreePage(parent.getChild(item.index));
		}
		else {
			sibling.readBTreePage(parent.getChild(item.index+1));
		}
		
		child.setKey(child.getKeyCount(), parent.getKey(item.index));
		child.setChild(child.getKeyCount()+1, sibling.getChild(0));
		child.keyCount++;
		
		for (int i = 0; i < sibling.keyCount; i++) {
			child.setKey(child.keyCount, sibling.getKey(i));
			child.setChild(child.keyCount+1, sibling.getChild(i+1));
			child.keyCount++;
		}
		
		child.writeBTreePage();
		sibling.freeBTreePage();
	}

	public int getKeyCount() {
		return this.keyCount;
	}

	public void freeBTreePage() throws IOException {
		BufferManager.getInstance().freePage(this.getPageNumber());
	}

	public void mergeLeaf(BTreePage sibling, BTreePage parent,
			StackItem item) throws IOException {
		BTreePage child = this;
		if (item.index == parent.getKeyCount()) {
			BTreePage tempPage = sibling;
			sibling = child;
			child = tempPage;
			item.index--;
			child.readBTreePage(parent.getChild(item.index));
		}
		else {
			sibling.readBTreePage(parent.getChild(item.index+1));
		}
		
		for (int i = 0; i < sibling.keyCount; i++) {
			child.appendRecord(sibling.getRecord(i));
		}
		child.nextPageNumber = sibling.nextPageNumber;
		child.writeBTreePage();
		sibling.freeBTreePage();
	}

	private void appendRecord(BTreeRecord record) {
		this.records.add(record.deepCopy());
		this.keyCount++;
	}
	
	public void redistributeLeaf(BTreePage sibling,
			BTreePage parent, int index) throws IOException {
		BTreePage child = this;
		
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

	public void redistributeNode(BTreePage sibling, BTreePage parent,
			int index) throws IOException {
		BTreePage child = this;
		
		int moveCount = (sibling.getKeyCount() - child.getKeyCount()) / 2;
		if (child.getKey(0).lessThan(sibling.getKey(0))) {
			child.setKey(child.getKeyCount(), parent.getKey(index));
			child.setChild(child.getKeyCount()+1, sibling.getChild(0));
			for (int i = 0; i < moveCount-1; i++) {
				child.setKey(child.getKeyCount()+i+1, sibling.getKey(i));
				child.setChild(child.getKeyCount()+i+2, sibling.getChild(i+1));
			}
			child.keyCount += moveCount;
			parent.setKey(index, sibling.getKey(moveCount-1));
			sibling.keyCount -= moveCount;
			
			sibling.setChild(0, sibling.getChild(moveCount));
			for (int i = 0; i < sibling.getKeyCount(); i++) {
				sibling.setKey(i, sibling.getKey(moveCount+i));
				sibling.setChild(i+1, sibling.getChild(moveCount+i+1));
			}
		}
		else {
			for (int i = child.getKeyCount(); i > 0; i--) {
				child.setKey(moveCount+i-1, child.getKey(i-1));
				child.setChild(moveCount+i, child.getChild(i));
			}
			child.setChild(moveCount, child.getChild(0));
			child.keyCount += moveCount;
			
			sibling.keyCount -= moveCount;
			child.setKey(moveCount-1, parent.getKey(index));
			for (int i = 0; i < moveCount-1; i++) {
				child.setKey(i, sibling.getKey(sibling.getKeyCount()+i+1));
				child.setChild(i+1, sibling.getChild(sibling.getKeyCount()+i+2));
			}
			child.setChild(0, sibling.getChild(sibling.getKeyCount()+1));
			parent.setKey(index, sibling.getKey(sibling.getKeyCount()));
		}
		
		child.writeBTreePage();
		sibling.writeBTreePage();
	}

}
