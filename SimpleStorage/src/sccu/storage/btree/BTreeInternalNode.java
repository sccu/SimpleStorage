package sccu.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;

import sccu.storage.btree.BTreeHeader.StackItem;
import sccu.storage.btree.key.BTreeKey;

public class BTreeInternalNode implements BTreePage {

	private int pageNumber;
	private int nextPageNumber;
	private int keyCount;
	private int[] data = null;

	public BTreeInternalNode() throws IOException {
		this.pageNumber = newPageNumber();
		this.nextPageNumber = -1;
		this.keyCount = 0;
		this.data = new int[BufferManager.getInstance().getPageSize()/4-3];
	}

	public BTreeInternalNode(int pageNumber, boolean leaf) {
		this.pageNumber = pageNumber;
		if (leaf) {
			this.nextPageNumber = 0;
		}
		else {
			this.nextPageNumber = -1;
		}
		
		this.keyCount = 0;
		if (!leaf) {
			this.data = new int[BufferManager.getInstance().getPageSize()/4-3];
		}
	}

	BTreeInternalNode(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		this.pageNumber = bb.getInt();
		this.nextPageNumber = bb.getInt();
		this.keyCount = bb.getInt();
		
		this.data = new int[BufferManager.getInstance().getPageSize()/4-3];
		this.setChild(0, bb.getInt());
		for (int i = 0; i < this.keyCount; i++) {
			this.setKey(i, new BTreeKey(bb.getInt()));
			this.setChild(i+1, bb.getInt());
		}
	}

	private static int newPageNumber() throws IOException {
		return BufferManager.getInstance().newPageNumber();
	}
	
	public void readBTreePage(int pageNo) throws IOException {
		byte[] buffer = BufferManager.getInstance().readPage(pageNo);
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		this.pageNumber = bb.getInt();
		this.nextPageNumber = bb.getInt();
		this.keyCount = bb.getInt();
		
		this.data = new int[BufferManager.getInstance().getPageSize()/4-3];
		this.setChild(0, bb.getInt());
		for (int i = 0; i < this.keyCount; i++) {
			this.setKey(i, new BTreeKey(bb.getInt()));
			this.setChild(i+1, bb.getInt());
		}
	}

	public void writeBTreePage() throws IOException {
		//this.validate();
		BufferManager.getInstance().writePage(pageNumber, this.toBytes());
	}

	private byte[] toBytes() {
		byte[] buffer = new byte[BufferManager.getInstance().getPageSize()];
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		
		bb.putInt(this.pageNumber);
		bb.putInt(this.nextPageNumber);
		bb.putInt(this.keyCount);
		
		// TODO: array를 통째로 복사. serializable 구현
		bb.putInt(this.getChild(0));
		for (int i = 0; i < this.keyCount; i++) {
			bb.put(this.getKey(i).toBytes());
			bb.putInt(this.getChild(i+1));
		}
		
		return buffer;
	}

	/*
	private void validate() throws IOException {
		for (int i = 0; i < this.keyCount - 1; i++) {
			if (!this.getKey(i).lessThan(this.getKey(i+1))) {
				throw new IOException("Invalid Page: " + this.getPageNumber() + " Index: " + i);
			}
		}
	}
	*/

	public int getPageNumber() {
		return pageNumber;
	}

	public void addKey(BTreeKey key, int rightPageNumber, int index) {
		for (int i = this.keyCount; i > index; i--) {
			setKey(i, getKey(i-1));
			setChild(i+1, getChild(i));
		}
		setKey(index, key);
		setChild(index+1, rightPageNumber);
		this.keyCount++;
	}

	public void setChild(int i, int child) {
		data[i*2] = child;
	}

	public int getChild(int i) {
		return data[i*2];
	}

	public void setKey(int i, BTreeKey key) {
		data[i*2+1] = ByteBuffer.wrap(key.toBytes()).getInt();
	}

	public BTreeKey getKey(int i) {
		return new BTreeKey(data[i*2+1]);
	}

	public void addRecord(BTreeRecord record, int index) {
		throw new UnsupportedOperationException();
	}

	public BTreeRecord getRecord(int index) {
		throw new UnsupportedOperationException();
	}
	
	public void removeRecord(int index) {
		throw new UnsupportedOperationException();
	}
	
	public void removeKey(int index) {
		for (int i = index; i < this.keyCount-1; i++) {
			this.setKey(i, this.getKey(i+1));
			this.setChild(i+1, this.getChild(i+2));
		}
		this.keyCount--;
	}

	public boolean isFull() {
		return this.keyCount == BTreeHeader.getOrder()-1;
	}

	public boolean isLeaf() {
		return false;
	}
	
	public void copyNode(BTreePage targetPage, int from, int count) {
		BTreeInternalNode target = (BTreeInternalNode)targetPage;
		for (int i = 0; i < count; i++) {
			target.setChild(i, this.getChild(i+from));
			target.setKey(i, this.getKey(i+from));
		}
		target.setChild(count, this.getChild(count+from));
		target.keyCount = count;
	}
	
	public BTreeKey splitLeaf(BTreeRecord record, BTreePage rightPage, int index) throws IOException {
		throw new UnsupportedOperationException();
	}

	public BTreeKey splitNode(BTreeKey key, int newChild, BTreePage rightPage, int index) throws IOException {
		BTreeInternalNode tempPage = new BTreeInternalNode(-2, false);
		this.copyNode(tempPage, 0, this.keyCount);
		tempPage.addKey(key, newChild, index);
		
		int midIndex = tempPage.keyCount / 2;
		BTreeKey midKey = tempPage.getKey(midIndex);
		
		tempPage.copyNode(this, 0, midIndex);
		this.writeBTreePage();
		
		tempPage.copyNode(rightPage, midIndex+1, tempPage.keyCount-midIndex-1);
		rightPage.writeBTreePage();
		
		return midKey;
	}

	public void merge(BTreePage sibling,
			BTreePage parent, StackItem item) throws IOException {
		BTreeInternalNode child = this;
		if (item.index == parent.getKeyCount()) {
			BTreeInternalNode tempPage = (BTreeInternalNode)sibling;
			sibling = child;
			child = tempPage;
			item.index--;
			child.readBTreePage(parent.getChild(item.index));
		}
		else {
			sibling = BufferManager.getBTreePage(parent.getChild(item.index+1));
		}
		
		child.setKey(child.getKeyCount(), parent.getKey(item.index));
		child.setChild(child.getKeyCount()+1, sibling.getChild(0));
		child.keyCount++;
		
		for (int i = 0; i < sibling.getKeyCount(); i++) {
			child.setKey(child.keyCount, sibling.getKey(i));
			child.setChild(child.keyCount+1, sibling.getChild(i+1));
			child.keyCount++;
		}
		
		child.writeBTreePage();
		sibling.freeBTreePage();
	}

	public int getKeyCount() {
		return this.keyCount;
	}

	public void freeBTreeInternalNode() throws IOException {
		BufferManager.getInstance().freePage(this.getPageNumber());
	}

	public void redistribute(BTreePage siblingPage, BTreePage parent, int index) throws IOException {
		BTreeInternalNode child = this;
		BTreeInternalNode sibling = (BTreeInternalNode) siblingPage;
		
		int moveCount = (sibling.getKeyCount() - child.getKeyCount()) / 2;
		if (child.getKey(0).lessThan(sibling.getKey(0))) {
			child.setKey(child.getKeyCount(), parent.getKey(index));
			child.setChild(child.getKeyCount()+1, sibling.getChild(0));
			for (int i = 0; i < moveCount-1; i++) {
				child.setKey(child.getKeyCount()+i+1, sibling.getKey(i));
				child.setChild(child.getKeyCount()+i+2, sibling.getChild(i+1));
			}
			child.keyCount += moveCount;
			parent.setKey(index, sibling.getKey(moveCount-1));
			sibling.keyCount -= moveCount;
			
			sibling.setChild(0, sibling.getChild(moveCount));
			for (int i = 0; i < sibling.getKeyCount(); i++) {
				sibling.setKey(i, sibling.getKey(moveCount+i));
				sibling.setChild(i+1, sibling.getChild(moveCount+i+1));
			}
		}
		else {
			for (int i = child.getKeyCount(); i > 0; i--) {
				child.setKey(moveCount+i-1, child.getKey(i-1));
				child.setChild(moveCount+i, child.getChild(i));
			}
			child.setChild(moveCount, child.getChild(0));
			child.keyCount += moveCount;
			
			sibling.keyCount -= moveCount;
			child.setKey(moveCount-1, parent.getKey(index));
			for (int i = 0; i < moveCount-1; i++) {
				child.setKey(i, sibling.getKey(sibling.getKeyCount()+i+1));
				child.setChild(i+1, sibling.getChild(sibling.getKeyCount()+i+2));
			}
			child.setChild(0, sibling.getChild(sibling.getKeyCount()+1));
			parent.setKey(index, sibling.getKey(sibling.getKeyCount()));
		}
		
		child.writeBTreePage();
		sibling.writeBTreePage();
	}

	@Override
	public void freeBTreePage() throws IOException {
		BufferManager.getInstance().freePage(this.getPageNumber());
	}

}
