package sccu.storage.simple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import sccu.storage.simple.BPlusTreeRecord.Key;

public class BPlusTreeMain {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		/*
		if (args.length < 2) {
			System.out.println("FILE open error");
			System.exit(-1);
		}
		*/
		
		BPlusTree tree = new BPlusTree();
		tree.initBTree("e:/temp/test.btree", 64, true);
		
		File commandFile = new File("./data/cmd.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(commandFile)));
		String line;
		while ((line = reader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line);
			String command = st.nextToken();
			String key = st.nextToken();
			switch(command.charAt(0)) {
			case 'i':
				tree.insertRecord(new BPlusTreeRecord(Integer.parseInt(key), st.nextToken()));
				break;
			case 'd':
				tree.deleteRecord(new Key(Integer.parseInt(key)));
				break;
			case 'r':
				BPlusTreeRecord record = new BPlusTreeRecord();
				tree.retrieveRecord(new Key(Integer.parseInt(key)), record);
				break;
			default:
				throw new Exception();
			}
		}
		
		tree.closeBTree();
	}

}
