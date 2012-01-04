package sccu.storage.simple;

import java.io.IOException;
import java.util.Stack;

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
	private int firstSequence;
	private int order;
	private int minKey;
	private int maxRecord;
	private Stack<StackItem> stack;
	private int minRecord;

	public void init(int rootPageNumber, int firstSequence) {
		this.rootPageNumber = rootPageNumber;
		this.firstSequence = firstSequence;
		this.order = (BufferManager.getInstance().getPageSize() - 4 * 4) / (4 * 2) + 1;
		this.minKey = this.order / 2 - 1 + this.order % 2;
		this.maxRecord = (BufferManager.getInstance().getPageSize() - 4 * 3) / BPlusTreeRecord.getSize();
		this.minRecord = this.maxRecord / 2;
		this.stack = new Stack<StackItem>();
	}

	public void loadPage() throws IOException {
		BufferManager.getInstance().readPage(0);
	}

	public void save() throws IOException {
		BufferManager.getInstance().saveHeaderPage(m_header.getBytes());
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
		if (page.getPageNumber() == this.firstSequence) {
			this.firstSequence = page.getNextPageNumber();
		}
	}
}
