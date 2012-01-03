package sccu.storage.simple;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BufferManager {
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
}
