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
		BPlusTreePage page = new BPlusTreePage(0, true);
		if (findRecord(record.getKey(), page)) {
			return false;
		}
		
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
					page.addKey(key, rightPageNumber, index);
				}
				finished = true;
			}
		}
		
		page.writeBTreePage();
		
		return true;
	}
	
	boolean deleteRecord(Key key) throws IOException {
		BPlusTreePage child = new BPlusTreePage(0, true);
		BPlusTreePage sibling = new BPlusTreePage(0, true);
		BPlusTreePage parent = new BPlusTreePage(0, true);
		
		if (!findRecord(key, child)) {
			return false;
		}
		
		StackItem item = null;
		
		boolean finished = false;
		while (!finished) {
			item = pop();
			if (child.isLeaf()) {
				child.removeRecord(item.index);
			}
			else {
				child.removeKey(item.index);
			}
			
			if (item.pageNumber == this.rootPageNumber) {
				if (child.getKeyCount() == 0 && !child.isLeaf()) {
					this.rootPageNumber = child.getChild(0);
					child.freeBTreePage();
					return true;
				}
				finished = true;
			}
			else if (child.getKeyCount() < this.getMin(child)) {
				item = peek();
				int i = this.selectSibling(sibling, parent, item);
				if (i == -1) {
					// merge
					if (child.isLeaf()) {
						
					}
					else {
						
					}
					
				}
				else {
					// redistribute
					if (child.isLeaf()) {
						
					}
					else {
						
					}
				}
				
				BPlusTreePage temp = child;
				child = parent;
				parent = temp;
			}
			else {
				finished = true;
			}
			
		}
		
		child.writeBTreePage();
		return true;
	}
	
	private int selectSibling(BPlusTreePage sibling, BPlusTreePage parent,
			StackItem item) {
		// TODO Auto-generated method stub
		return 0;
	}

	private int getMin(BPlusTreePage page) {
		return page.isLeaf() ? this.minRecord : this.minKey;
	}

	private boolean findRecord(Key key, BPlusTreePage page) {
		return false;
	}
	
}
