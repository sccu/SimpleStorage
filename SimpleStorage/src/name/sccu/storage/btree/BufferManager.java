package name.sccu.storage.btree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

class BufferManager {
	private static final int MAX_PAGE_SIZE = 10000;
	private final static BufferManager m_bufferManager = new BufferManager();
	private RandomAccessFile file = null;
	private int pageSize;
	private int maxPageNumber;
	private int lastFreePageNumber;
	
	private boolean[] used = new boolean[MAX_PAGE_SIZE];
	
	void initBufferManager(String filename, int pageSize) throws IOException {
		File file = new File(filename);
		if (!file.exists()) {
			file.createNewFile();
		}
		
		this.file = new RandomAccessFile(file, "rw");
		
		this.pageSize = pageSize;
		this.maxPageNumber = 0;
		this.lastFreePageNumber = -1;
	}
	
	void close() throws IOException {
		this.file.close();
	}
	
	static BufferManager getInstance() {
		return m_bufferManager;
	}

	private void writePage(int pageNumber, byte[] bytes) throws IOException {
		file.seek(pageNumber * pageSize);
		file.write(bytes);
	}
	
	void writePage(BTreePage page) throws IOException {
		writePage(page.getPageNumber(), page.toBytes());
	}

	int getPageSize() {
		return pageSize;
	}

	int getMaxPageNumber() {
		return maxPageNumber;
	}

	int getLastFreePageNumber() {
		return lastFreePageNumber;
	}

	private byte[] readPage(int pageNumber) throws IOException {
		byte[] buffer = new byte[this.pageSize];
		file.seek(pageNumber * this.pageSize);
		file.read(buffer);
		return buffer;
	}

	void saveHeaderPage(byte[] bytes) throws IOException {
		writePage(0, bytes);
	}

	int newPageNumber() throws IOException {
		int freePageNumber;
		if (this.lastFreePageNumber == -1) {
			this.maxPageNumber++;
			freePageNumber = this.maxPageNumber;
		}
		else {
			freePageNumber = this.lastFreePageNumber;
			
			this.file.seek(freePageNumber * this.pageSize + 4);
			int nextPageNumber = this.file.readInt();
			if (nextPageNumber == -1) {
				this.lastFreePageNumber = -1;
			}
			else {
				this.lastFreePageNumber = nextPageNumber;
			}
		}
		
		if (used[freePageNumber]) {
			throw new IOException(freePageNumber + " already used.");
		}
		used[freePageNumber] = true;
		return freePageNumber;
	}
	
	byte[] loadHeaderPage() throws IOException {
		byte[] buffer = this.readPage(0);
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		this.pageSize = bb.getInt();
		this.maxPageNumber = bb.getInt();
		this.lastFreePageNumber = bb.getInt();
		
		return buffer;
	}

	void resetDebugData() {
		used = new boolean[MAX_PAGE_SIZE];
	}

	BTreePage getBTreePage(int pageNumber) throws IOException {
		byte[] bytes = BufferManager.getInstance().readPage(pageNumber);
		return BTreePage.Factory.create(bytes);
	}

	/**
	 * @param sibling
	 * @throws IOException 
	 */
	void freePage(BTreePage page) throws IOException {
		if (!used[page.getPageNumber()]) {
			throw new IOException(page.getPageNumber() + " already used.");
		}
		used[page.getPageNumber()] = false;
		
		this.file.seek(page.getPageNumber() * this.pageSize + 4);
		this.file.writeInt(this.lastFreePageNumber);
		this.lastFreePageNumber = page.getPageNumber();
	}
	
}
