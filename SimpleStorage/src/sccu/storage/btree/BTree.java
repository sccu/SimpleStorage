package sccu.storage.btree;

import java.io.IOException;

import sccu.storage.btree.key.BTreeKey;

public class BTree {
	private BTreeHeader header = new BTreeHeader();

	public void initBTree(String filename, int pageSize, boolean newStart)
			throws IOException {
		BufferManager.getInstance().initBufferManager(filename, pageSize);
		if (newStart) {
			BTreePage page = new BTreePage(true );
			page.writeBTreePage();
			header.init(page.getPageNumber(), page.getPageNumber());
			BufferManager.getInstance().resetDebugData();
		} else {
			header.loadBTreeHeaderPage();
		}
	}

	public void closeBTree() throws IOException {
		header.saveBTreeHeaderPage();
		BufferManager.getInstance().close();
	}

	public boolean insertRecord(BTreeRecord record) throws IOException {
		return header.insertRecord(record);
	}

	public boolean deleteRecord(BTreeKey key) throws IOException {
		return header.deleteRecord(key);
	}

	public boolean retrieveRecord(BTreeKey key, BTreeRecord record)
			throws IOException {
		BTreePage page = new BTreePage();
		boolean found = header.findRecord(key, page);
		if (found) {
			int i = header.peek().index;
			record.copyFrom(page.getRecord(i));
		}
		return found;
	}

}
