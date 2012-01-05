package sccu.storage.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

	private static int newPageNumber() throws IOException {
		return BufferManager.getInstance().newPageNumber();
	}
	
	public void read(int pageNo) throws IOException {
		byte[] buffer = BufferManager.getInstance().readPage(pageNo);
		this.pageNumber = pageNo;
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.BIG_ENDIAN);
		this.nextPageNumber = bb.getInt();
	}

	public void write() throws IOException {
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

	private void setChild(int i, int child) {
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
			((BPlusTreeRecord) this.getRecord(i)).copy(this.getRecord(i-1));
		}
		this.getRecord(index).copy(record);
		this.keyCount++;
	}

	private BPlusTreeRecord getRecord(int i) {
		return null;
	}
}
