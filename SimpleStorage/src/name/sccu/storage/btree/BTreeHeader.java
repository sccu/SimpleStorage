package name.sccu.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import name.sccu.storage.btree.BTreePage.BTreePageHolder;
import name.sccu.storage.btree.key.BTreeKey;


public class BTreeHeader {

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
		maxRecord = (BufferManager.getInstance().getPageSize() - 4 * 3) / BTreeRecord.getSize();
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

	public static int getMaxRecord() {
		return maxRecord;
	}

	public static int getOrder() {
		return order;
	}

	public boolean insertRecord(BTreeRecord record) throws IOException {
		BTreePageHolder pageHolder = new BTreePageHolder();
		if (findRecord(record.getKey(), pageHolder)) {
			return false;
		}
		
		int index = 0;
		int leftPageNumber = 0;
		BTreePage page = pageHolder.get();	// At this time, page refers a leaf.
		BTreePage rightPage = null;
		BTreeKey key = record.getKey();
		
		boolean finished = false;
		while (!finished) {
			if (stack.empty()) {
				// 새로운 루트 생성하면서 트리 높이가 1 증가
				leftPageNumber = this.rootPageNumber;
				page = (BTreePage)new BTreeInternalNode();
				this.rootPageNumber = page.getPageNumber();
				page.setChild(0, leftPageNumber);
				index = 0;
			}
			else {
				BTreeHeader.StackItem item = stack.pop();
				index = item.index;
				if (rightPage != null && rightPage.getPageNumber() != HEADER_PAGE_NUMBER) {
					page = BufferManager.getBTreePage(item.pageNumber);
				}
			}
			
			if (page.isFull()) {
				if (page.isLeaf()) {
					rightPage = new BTreeLeafNode();
					key = page.splitLeaf(record, rightPage, index);
				}
				else {
					int newChild = rightPage.getPageNumber();
					rightPage = new BTreeInternalNode();
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
	
	boolean deleteRecord(BTreeKey key) throws IOException {
		BTreePageHolder childHolder = new BTreePageHolder();
		BTreePageHolder siblingHolder = new BTreePageHolder();
		BTreePage parent;
		
		if (!findRecord(key, childHolder)) {
			return false;
		}
		
		BTreePage child = childHolder.get();
		StackItem item = null;
		
		boolean finished = false;
		while (!finished) {
			item = this.stack.pop();
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
			else if (child.getKeyCount() < BTreeHeader.getMin(child)) {
				item = this.stack.peek();
				parent = BufferManager.getBTreePage(item.pageNumber);
				int i = this.selectSibling(siblingHolder, parent, item);
				if (i == -1) {
					// merge
					child.merge(siblingHolder.get(), parent, item);
				}
				else {
					// redistribute
					child.redistribute(siblingHolder.get(), parent, i);
					finished = true;
				}
				
				child = parent;
			}
			else {
				finished = true;
			}
			
		}
		
		child.writeBTreePage();
		return true;
	}
	
	private int selectSibling(BTreePageHolder siblingHolder, BTreePage parent,
			StackItem item) throws IOException {
		int i = -1;
		BTreePage sibling;
		if (item.index == 0) {
			sibling = BufferManager.getBTreePage(parent.getChild(1));
			if (sibling.getKeyCount() > BTreeHeader.getMin(sibling)) {
				i = item.index;
			}
		}
		else if (item.index == parent.getKeyCount()) {
			sibling = BufferManager.getBTreePage(parent.getChild(item.index-1));
			if (sibling.getKeyCount() > BTreeHeader.getMin(sibling)) {
				i = item.index - 1;
			}
		}
		else {
			sibling = BufferManager.getBTreePage(parent.getChild(item.index+1));
			if (sibling.getKeyCount() > BTreeHeader.getMin(sibling)) {
				i = item.index;
			}
			else {
				sibling = BufferManager.getBTreePage(parent.getChild(item.index-1));
				if (sibling.getKeyCount() > BTreeHeader.getMin(sibling)) {
					i = item.index - 1;
				}
			}
		}
		
		siblingHolder.set(sibling);
		
		return i;
	}
	
	private static int getMin(BTreePage page) {
		return page.isLeaf() ? minRecord : minKey;
	}

	boolean findRecord(BTreeKey key, BTreePageHolder pageHolder) throws IOException {
		int currentPageNumber = this.rootPageNumber;
		this.stack.clear();
		BTreePage page = BufferManager.getBTreePage(currentPageNumber);
		int i;
		while (!page.isLeaf()) {
			for (i = 0; i < page.getKeyCount() && page.getKey(i).lessThan(key); i++) {
				;
			}
			this.stack.push(new StackItem(page.getPageNumber(), i));
			currentPageNumber = page.getChild(i);
			page = BufferManager.getBTreePage(currentPageNumber);
		}
		
		for (i = 0; i < page.getKeyCount() && page.getRecord(i).getKey().lessThan(key); i++) {
			;
		}
		this.stack.push(new StackItem(page.getPageNumber(), i));
		
		pageHolder.set(page);
		return ((i < page.getKeyCount()) && key.equals(page.getRecord(i).getKey()));
	}

	/**
	 * FIXME Should remove this method. 
	 * 
	 * @return the stack
	 */
	Stack<StackItem> getStack() {
		return stack;
	}

}
