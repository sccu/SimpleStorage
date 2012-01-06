package sccu.storage.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTreeHeader {

	public static class StackItem {
		public StackItem(int pageNumber, int index) {
			this.pageNumber = pageNumber;
			this.index = index;
		}
		int pageNumber;
		int index;
	}

	private static BPlusTreeHeader m_header = new BPlusTreeHeader();

	public static BPlusTreeHeader getInstance() {
		return m_header;
	}

	private int rootPageNumber;
	private int firstSequencePage;
	private int order;
	private int minKey;
	private int maxRecord;
	private Stack<StackItem> stack;
	private int minRecord;

	public void init(int rootPageNumber, int firstSequence) {
		this.rootPageNumber = rootPageNumber;
		this.firstSequencePage = firstSequence;
		this.order = (BufferManager.getInstance().getPageSize() - 4 * 4) / (4 * 2) + 1;
		this.minKey = this.order / 2 - 1 + this.order % 2;
		this.maxRecord = (BufferManager.getInstance().getPageSize() - 4 * 3) / BPlusTreeRecord.getSize();
		this.minRecord = this.maxRecord / 2;
		this.stack = new Stack<StackItem>();
	}

	public void loadBTreeHeaderPage() throws IOException {
		BufferManager.getInstance().loadHeaderPage();
	}

	public void saveBTreeHeaderPage() throws IOException {
		byte[] buffer = new byte[BufferManager.getInstance().getPageSize()];
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.putInt(BufferManager.getInstance().getPageSize());
		bb.putInt(BufferManager.getInstance().getMaxPageNumber());
		bb.putInt(BufferManager.getInstance().getLastFreePageNumber());
		bb.putInt(this.rootPageNumber);
		bb.putInt(this.firstSequencePage);
		
		BufferManager.getInstance().saveHeaderPage(buffer);
	}

	private byte[] getBytes() {
		return null;
	}

	public void push(StackItem stackItem) {
		stack.push(stackItem);
	}

	public StackItem pop() {
		return stack.pop();
	}
	
	public StackItem peek() {
		return stack.peek();
	}

	public void freePage(BPlusTreePage page) {
		if (page.getPageNumber() == this.firstSequencePage) {
			this.firstSequencePage = page.getNextPageNumber();
		}
	}

	public int getMaxRecord() {
		return this.maxRecord;
	}

	public int getOrder() {
		return this.order;
	}

	public boolean insertRecord(BPlusTreeRecord record) throws IOException {
		if (findRecord(record.getKey())) {
			return false;
		}
		
		BPlusTreePage page = null;
		int index = 0;
		int leftPageNumber = 0;
		int rightPageNumber = 0;
		Key key = record.getKey();
		
		boolean finished = false;
		while (!finished) {
			if (stack.peek() == null) {
				// 새로운 루트 생성하면서 트리 높이가 1 증가
				leftPageNumber = this.rootPageNumber;
				page = new BPlusTreePage(false);
				this.rootPageNumber = page.getPageNumber();
				page.setChild(0, leftPageNumber);
				index = 0;
			}
			else {
				BPlusTreeHeader.StackItem item = stack.pop();
				index = item.index;
				if (rightPageNumber != 0) {
					page = new BPlusTreePage();
					page.readBTreePage(item.pageNumber);
				}
			}
			
			if (page.isFull()) {
				if (page.isLeaf()) {
					key = page.splitLeaf(record, index);
				}
				else {
					key = page.splitNode(key, rightPageNumber, index);
				}
				rightPageNumber = page.getPageNumber();
			}
			else {
				if (page.isLeaf()) {
					page.addRecord(record, index);
				}
				else {
					page.addKey(key.getInt(), rightPageNumber, index);
				}
				finished = true;
			}
		}
		
		page.writeBTreePage();
		
		return true;
	}
	
	private boolean findRecord(Key key) {
		return false;
	}
	
}
