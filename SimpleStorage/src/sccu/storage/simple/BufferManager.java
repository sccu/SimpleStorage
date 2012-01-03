package sccu.storage.simple;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BufferManager {
	private final static BufferManager m_bufferManager = new BufferManager();
	private RandomAccessFile file = null;
	private int pageSize;
	private int maxPageNumber;
	private int lastFreePageNumber;
	
	public void init(String filename, int pageSize) throws IOException {
		File file = new File("filename", "rb+");
		if (!file.exists()) {
			file.createNewFile();
		}
		
		this.file = new RandomAccessFile(file, "rb+");
		
		this.pageSize = pageSize;
		this.maxPageNumber = 0;
		this.lastFreePageNumber = -1;
	}
	
	public void close() throws IOException {
		this.file.close();
	}
	
	public static BufferManager getInstance() {
		return m_bufferManager;
	}

	public void writePage(int pageNumber, byte[] bytes) throws IOException {
		file.seek(pageNumber * pageSize);
		file.write(bytes);
	}

	public int getPageSize() {
		return pageSize;
	}

	public void readPage(int pageNumber) throws IOException {
		byte[] buffer = new byte[this.pageSize];
		file.seek(pageNumber * this.pageSize);
		file.read(buffer);
	}

	public void saveHeaderPage(byte[] bytes) {
		
	}
}
