package sccu.storage.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTreeHeader {

	private static final int HEADER_PAGE_NUMBER = 0;
	private static final int ROOT_PAGE_POSITION = 4 * 3;
	private static final int FIRST_SEQ_POSITION = 4 * 4;

	public static class StackItem {
		public StackItem(int pageNumber, int index) {
			this.pageNumber = pageNumber;
			this.index = index;
		}
		int pageNumber;
		int index;
	}

	private int rootPageNumber;
	private int firstSequencePage;
	private static int order;
	private static int minKey;
	private static int maxRecord;
	private Stack<StackItem> stack;
	private static int minRecord;

	public void init(int rootPageNumber, int firstSequence) {
		this.rootPageNumber = rootPageNumber;
		this.firstSequencePage = firstSequence;
		order = (BufferManager.getInstance().getPageSize() - 4 * 4) / (4 * 2) + 1;
		minKey = order / 2 - 1 + order % 2;
		maxRecord = (BufferManager.getInstance().getPageSize() - 4 * 3) / BPlusTreeRecord.getSize();
		minRecord = maxRecord / 2;
		this.stack = new Stack<StackItem>();
	}

	public void loadBTreeHeaderPage() throws IOException {
		byte[] buffer = BufferManager.getInstance().loadHeaderPage();
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		this.init(bb.getInt(ROOT_PAGE_POSITION), bb.getInt(FIRST_SEQ_POSITION));
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

	public void push(StackItem stackItem) {
		stack.push(stackItem);
	}

	public StackItem pop() {
		return stack.pop();
	}
	
	public StackItem peek() {
		return stack.peek();
	}
	
	public void freePage(BPlusTreePage page) throws IOException {
		if (page.getPageNumber() == this.firstSequencePage) {
			this.firstSequencePage = page.getNextPageNumber();
		}
		BufferManager.getInstance().freePage(page.getPageNumber());
	}

	public static int getMaxRecord() {
		return maxRecord;
	}

	public static int getOrder() {
		return order;
	}

	public boolean insertRecord(BPlusTreeRecord record) throws IOException {
		BPlusTreePage page = new BPlusTreePage(0, true);
		if (findRecord(record.getKey(), page)) {
			return false;
		}
		
		int index = 0;
		int leftPageNumber = 0;
		BPlusTreePage rightPage = null;
		Key key = record.getKey();
		
		boolean finished = false;
		while (!finished) {
			if (stack.empty()) {
				// ���ο� ��Ʈ �����ϸ鼭 Ʈ�� ���̰� 1 ����
				leftPageNumber = this.rootPageNumber;
				page = new BPlusTreePage(false);
				this.rootPageNumber = page.getPageNumber();
				page.setChild(0, leftPageNumber);
				index = 0;
			}
			else {
				BPlusTreeHeader.StackItem item = stack.pop();
				index = item.index;
				if (rightPage != null && rightPage.getPageNumber() != HEADER_PAGE_NUMBER) {
					page = new BPlusTreePage();
					page.readBTreePage(item.pageNumber);
				}
			}
			
			if (page.isFull()) {
				if (page.isLeaf()) {
					rightPage = new BPlusTreePage(true);
					key = page.splitLeaf(record, rightPage, index);
				}
				else {
					int newChild = rightPage.getPageNumber();
					rightPage = new BPlusTreePage(false);
					key = page.splitNode(key, newChild, rightPage, index);
				}
			}
			else {
				if (page.isLeaf()) {
					page.addRecord(record, index);
				}
				else {
					page.addKey(key, rightPage.getPageNumber(), index);
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
			else if (child.getKeyCount() < BPlusTreeHeader.getMin(child)) {
				item = peek();
				int i = this.selectSibling(sibling, parent, item);
				if (i == -1) {
					// merge
					if (child.isLeaf()) {
						child.mergeLeaf(sibling, parent, item);
					}
					else {
						child.mergeNode(sibling, parent, item);
					}
				}
				else {
					// redistribute
					if (child.isLeaf()) {
						child.redistributeLeaf(sibling, parent, i);
					}
					else {
						child.redistributeNode(sibling, parent, i);
					}
					finished = true;
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
			StackItem item) throws IOException {
		int i = -1;
		parent.readBTreePage(item.pageNumber);
		if (item.index == 0) {
			sibling.readBTreePage(parent.getChild(1));
			if (sibling.getKeyCount() > BPlusTreeHeader.getMin(sibling)) {
				i = item.index;
			}
		}
		else if (item.index == parent.getKeyCount()) {
			sibling.readBTreePage(parent.getChild(item.index-1));
			if (sibling.getKeyCount() > BPlusTreeHeader.getMin(sibling)) {
				i = item.index - 1;
			}
		}
		else {
			sibling.readBTreePage(parent.getChild(item.index+1));
			if (sibling.getKeyCount() > BPlusTreeHeader.getMin(sibling)) {
				i = item.index;
			}
			else {
				sibling.readBTreePage(parent.getChild(item.index-1));
				if (sibling.getKeyCount() > BPlusTreeHeader.getMin(sibling)) {
					i = item.index - 1;
				}
			}
		}
		
		return i;
	}
	
	private static int getMin(BPlusTreePage page) {
		return page.isLeaf() ? minRecord : minKey;
	}

	boolean findRecord(Key key, BPlusTreePage page) throws IOException {
		int currentPageNumber = this.rootPageNumber;
		this.stack.clear();
		page.readBTreePage(currentPageNumber);
		int i;
		while (!page.isLeaf()) {
			for (i = 0; i < page.getKeyCount() && page.getKey(i).lessThan(key); i++) {
				;
			}
			this.push(new StackItem(page.getPageNumber(), i));
			currentPageNumber = page.getChild(i);
			page.readBTreePage(currentPageNumber);
		}
		
		for (i = 0; i < page.getKeyCount() && page.getRecord(i).getKey().lessThan(key); i++) {
			;
		}
		this.push(new StackItem(page.getPageNumber(), i));
		
		return ((i < page.getKeyCount()) && key.equals(page.getRecord(i).getKey()));
	}
	
}
