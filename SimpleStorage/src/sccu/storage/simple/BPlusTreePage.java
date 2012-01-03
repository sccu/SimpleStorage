package sccu.storage.simple;

import java.io.IOException;

class BPlusTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;

	public BPlusTreePage(boolean leaf) {
		this.pageNumber = newBPlusTreePageNumber();
		if (leaf) {
			this.nextPageNumber = 0;
		}
		else {
			this.nextPageNumber = -1;
		}
		
		this.keyCount = 0;
	}

	private static int newBPlusTreePageNumber() {
		return 0;
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
