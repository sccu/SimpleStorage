package sccu.storage.simple;

import java.io.IOException;

public class BPlusTree {
	public void init(String filename, int pageSize, boolean newStart) throws IOException {
		BufferManager.getInstance().init(filename, pageSize);
		if (newStart) {
			BPlusTreePage page = new BPlusTreePage(true);
			page.write();
			BPlusTreeHeader.getInstance().init(page.getPageNumber(), page.getPageNumber());
		}
		else {
			BPlusTreeHeader.getInstance().loadPage();
		}
	}
	
	public void close() throws IOException {
		BPlusTreeHeader.getInstance().save();
		BufferManager.getInstance().close();
	}
	
	public void push(int pageNumber, int index) {
		BPlusTreeHeader.getInstance().push(new BPlusTreeHeader.StackItem(pageNumber, index));
	}
	
	public BPlusTreeHeader.StackItem pop() {
		return BPlusTreeHeader.getInstance().pop();
	}
	
	public BPlusTreeHeader.StackItem peek() {
		return BPlusTreeHeader.getInstance().peek();
	}
	
	public void freePage(BPlusTreePage page) throws IOException {
		BPlusTreeHeader.getInstance().freePage(page);
		BufferManager.getInstance().freePage(page.getPageNumber());
	}
	
}
