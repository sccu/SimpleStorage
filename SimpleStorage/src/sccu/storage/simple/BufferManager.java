package sccu.storage.simple;

import java.io.File;
import java.io.IOException;

public class BufferManager {
	private File file = null;
	private int pageSize;
	private int maxPageNumber;
	private int lastFreePageNumber;
	
	void init(String filename, int pageSize) throws IOException {
		file = new File("filename", "rb+");
		if (!file.exists()) {
			file.createNewFile();
		}
		
		this.pageSize = pageSize;
		this.maxPageNumber = 0;
		this.lastFreePageNumber = -1;
	}
}
