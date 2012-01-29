package sccu.storage.btree;

import java.util.ArrayList;
import java.util.List;

public class BTreeFactory {
	
	public enum DataType {
		STRING, INTEGER;
	}
	
	private static class ColumnDefinition {
		private DataType dataType;
		private int size;

		private ColumnDefinition(DataType dataType, int size) {
			this.dataType = dataType;
			this.size = size;
		}
	}

	private List<ColumnDefinition> columnDefinitions = new ArrayList<ColumnDefinition>();

	public BTree createBTree() {
		return null;
	}

	public void addColumn(ColumnDefinition def) {
		this.columnDefinitions.add(def);
	}

}
