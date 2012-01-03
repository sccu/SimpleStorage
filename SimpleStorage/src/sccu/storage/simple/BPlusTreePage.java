package sccu.storage.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BPlusTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;

	public BPlusTreePage(boolean leaf) throws IOException {
		this.pageNumber = newBPlusTreePageNumber();
		if (leaf) {
			this.nextPageNumber = 0;
		}
		else {
			this.nextPageNumber = -1;
		}
		
		this.keyCount = 0;
	}

	private static int newBPlusTreePageNumber() throws IOException {
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

}
