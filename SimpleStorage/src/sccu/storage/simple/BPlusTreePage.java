package sccu.storage.simple;

import java.io.IOException;
import java.nio.ByteBuffer;

import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;
	private int[] data = new int[0];

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
	
	public void addKey(int key, int rightPageNumber, int index) {
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

	private int getChild(int i) {
		return data[i*2];
	}

	private void setKey(int i, int key) {
		data[i*2+1] = key;
	}

	private int getKey(int i) {
		return data[i*2+1];
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
	
	public void copyKey(int source, int target) {
		this.setKey(target, this.getKey(source));
		this.setChild(target+1, this.getChild(source+1));
	}

	public Key splitLeaf(BPlusTreeRecord record, int index) {
		return null;
	}

	public Key splitNode(Key key, int rightPageNumber, int index) {
		BPlusTreePage tempPage = new BPlusTreePage(-2, false);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addKey(key.getInt(), rightPageNumber, index);
		
		return null;
	}
	
}
