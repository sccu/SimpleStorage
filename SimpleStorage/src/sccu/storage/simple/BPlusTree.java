package sccu.storage.simple;

import java.io.IOException;

import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTree {
	private BPlusTreeHeader header = new BPlusTreeHeader();
	
	public void initBTree(String filename, int pageSize, boolean newStart) throws IOException {
		BufferManager.getInstance().initBufferManager(filename, pageSize);
		if (newStart) {
			BPlusTreePage page = new BPlusTreePage(true);
			page.writeBTreePage();
			header.init(page.getPageNumber(), page.getPageNumber());
			BufferManager.getInstance().resetDebugData();
		}
		else {
			header.loadBTreeHeaderPage();
		}
	}
	
	public void closeBTree() throws IOException {
		header.saveBTreeHeaderPage();
		BufferManager.getInstance().close();
	}
	
	public boolean insertRecord(BPlusTreeRecord record) throws IOException {
		return header.insertRecord(record);
	}
	
	public boolean deleteRecord(Key key) throws IOException {
		return header.deleteRecord(key);
	}

	public boolean retrieveRecord(Key key, BPlusTreeRecord record) throws IOException {
		BPlusTreePage page = new BPlusTreePage();
		boolean found = header.findRecord(key, page);
		if (found) {
			int i = header.peek().index;
			record.copyFrom(page.getRecord(i));
		}
		return found;
	}

}
