package sccu.storage.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import sccu.storage.simple.BPlusTreeHeader.StackItem;
import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;
	private int[] data = new int[0];
	private List<BPlusTreeRecord> records = new ArrayList<BPlusTreeRecord>();

	public BPlusTreePage(boolean leaf) throws IOException {
		this.pageNumber = newPageNumber();
		if (leaf) {
			this.nextPageNumber = 0;
		}
		else {
			this.nextPageNumber = -1;
		}
		
		this.keyCount = 0;
	}

	public BPlusTreePage(int pageNumber, boolean leaf) {
		this.pageNumber = pageNumber;
	}

	public BPlusTreePage() {
		// TODO Auto-generated constructor stub
	}

	private static int newPageNumber() throws IOException {
		return BufferManager.getInstance().newPageNumber();
	}
	
	public void readBTreePage(int pageNo) throws IOException {
		byte[] buffer = BufferManager.getInstance().readPage(pageNo);
		this.pageNumber = pageNo;
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		this.nextPageNumber = bb.getInt();
	}

	public void writeBTreePage() throws IOException {
		BufferManager.getInstance().writePage(pageNumber, this.toBytes());
	}

	private byte[] toBytes() {
		return null;
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

	public void addRecord(BPlusTreeRecord record, int index) {
		for (int i = this.keyCount; i > index; i--) {
			((BPlusTreeRecord) this.getRecord(i)).copyFrom(this.getRecord(i-1));
		}
		this.getRecord(index).copyFrom(record);
		this.keyCount++;
	}

	private BPlusTreeRecord getRecord(int i) {
		return null;
	}
	
	public void removeRecord(int index) {
		for (int i = index; i < this.keyCount-1; i++) {
			this.getRecord(i).copyFrom(this.getRecord(i+1));
		}
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
			return this.keyCount == BPlusTreeHeader.getInstance().getMaxRecord();
		}
		else {
			return this.keyCount == BPlusTreeHeader.getInstance().getOrder()-1;
		}
	}

	boolean isLeaf() {
		return this.nextPageNumber != -1;
	}
	
	public void copyNode(BPlusTreePage targetPage, int from, int count) {
		targetPage.keyCount = 0;
		if (this.isLeaf()) {
			for (int i = 0; i < count; i++) {
				targetPage.getRecord(i).copyFrom(this.getRecord(i+from));
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
	
	public Key splitLeaf(BPlusTreeRecord record, int index) throws IOException {
		BPlusTreePage tempPage = new BPlusTreePage(-2, true);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addRecord(record, index);
		
		//int midIndex = tempPage.keyCount/2 - 1 + tempPage.keyCount%2;
		int midIndex = (tempPage.keyCount-1) / 2;
		Key midKey = tempPage.getRecord(midIndex).getKey();
		
		BPlusTreePage newPage = new BPlusTreePage(true);
		newPage.nextPageNumber = this.nextPageNumber;
		this.nextPageNumber = newPage.pageNumber;
		
		tempPage.copyNode(this, 0, midIndex+1);
		this.writeBTreePage();
		
		tempPage.copyNode(newPage, midIndex+1, tempPage.keyCount-midIndex-1);
		newPage.writeBTreePage();
		
		return midKey;
	}

	public Key splitNode(Key key, int rightPageNumber, int index) throws IOException {
		BPlusTreePage tempPage = new BPlusTreePage(-2, false);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addKey(key, rightPageNumber, index);
		
		int midIndex = tempPage.keyCount / 2;
		Key midKey = tempPage.getKey(midIndex);
		
		tempPage.copyNode(this, 0, midIndex);
		this.writeBTreePage();
		
		BPlusTreePage page = new BPlusTreePage(false);
		tempPage.copyNode(page, midIndex+1, tempPage.keyCount-midIndex-1);
		page.writeBTreePage();
		
		return midKey;
	}

	public void mergeNode(BPlusTreePage sibling,
			BPlusTreePage parent, StackItem item) throws IOException {
		BPlusTreePage child = this;
		if (item.index == parent.getKeyCount()) {
			BPlusTreePage tempPage = sibling;
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

	public void freeBTreePage() {
		
	}

	public void mergeLeaf(BPlusTreePage sibling, BPlusTreePage parent,
			StackItem item) throws IOException {
		BPlusTreePage child = this;
		if (item.index == parent.getKeyCount()) {
			BPlusTreePage tempPage = sibling;
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

	private void appendRecord(BPlusTreeRecord record) {
		this.records.add(record.deepCopy());
		this.keyCount++;
	}
	
	public void redistributeLeaf(BPlusTreePage sibling,
			BPlusTreePage parent, int i) {
		BPlusTreePage child = this;
		
		int moveCount = (sibling.getKeyCount() - child.getKeyCount()) / 2;
		
		if (child.getRecord(0).getKey().getInt() < sibling.getRecord(0).getKey().getInt()) {
			
		}
		
	}

}
