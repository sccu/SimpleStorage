package sccu.storage.simple;

import java.io.IOException;

import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTree {
	public void initBTree(String filename, int pageSize, boolean newStart) throws IOException {
		BufferManager.getInstance().initBufferManager(filename, pageSize);
		if (newStart) {
			BPlusTreePage page = new BPlusTreePage(true);
			page.writeBTreePage();
			BPlusTreeHeader.getInstance().init(page.getPageNumber(), page.getPageNumber());
		}
		else {
			BPlusTreeHeader.getInstance().loadBTreeHeaderPage();
		}
	}
	
	public void closeBTree() throws IOException {
		BPlusTreeHeader.getInstance().saveBTreeHeaderPage();
		BufferManager.getInstance().close();
	}
	
	public boolean insertRecord(BPlusTreeRecord record) throws IOException {
		return BPlusTreeHeader.getInstance().insertRecord(record);
	}
	
	public boolean deleteRecord(Key key) throws IOException {
		return BPlusTreeHeader.getInstance().deleteRecord(key);
	}

	public boolean retrieveRecord(Key key, BPlusTreeRecord record) throws IOException {
		BPlusTreePage page = new BPlusTreePage();
		boolean found = BPlusTreeHeader.getInstance().findRecord(key, page);
		if (found) {
			int i = BPlusTreeHeader.getInstance().peek().index;
			record.copyFrom(page.getRecord(i));
		}
		return found;
	}

}
