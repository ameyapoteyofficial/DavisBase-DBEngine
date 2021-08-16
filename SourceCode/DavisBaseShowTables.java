import static java.lang.System.out;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;

public class DavisBaseShowTables {
	
	public static void parseShow(String showString) {
		ArrayList<String> cmdTokens = new ArrayList<String>(Arrays.asList(showString.split("\s+")));
		if (cmdTokens.size() == 2 && cmdTokens.get(1).equals("tables")) {
			DavisBaseShowTables.showTables();
			DavisBaseShowTables.showColumns();
		} else {
			System.out.println("Invalid show command");
			System.out.println("USAGE: 			show tables;");
		}
	}
	
	/**
	 * Method to display list of tables from "davisbase_tables.tbl"
	 */
	public static void showTables() {
		try {
			RandomAccessFile TbCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			System.out.println();
			System.out.println("rowid       table_name");
			System.out.println("--------------------------------");
			displayTable(TbCatalog);
			TbCatalog.close();
		} catch (Exception e) {
			out.println("Error getting table names from davisbase_tables.tbl");
			out.println(e);
		}
	}

	/**
	 * Method to display list of columns of all tables from "davisbase_columns.tbl"
	 */
	public static void showColumns() {
		try {
			RandomAccessFile colmCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Formatter f = new Formatter();			// create a Formatter to format the output in proper alignment
			System.out.println();
			f.format("%s %18s %18s %18s %18s %18s\n", "rowid", "table_name", "column_name", "data_type", "ordinal_pos", "is_nullable");
			System.out.print(f);					// print the formatted line
			System.out.println("------------------------------------------------------------------------------------------------------");
			displayTable(colmCatalog);
			colmCatalog.close();
			System.out.println();
		} catch (Exception e) {
			out.println("Error getting column names from davisbase_columns.tbl");
			out.println(e);
		}
	}
	
	/**
	 * Generic method to display any record
	 * @param ref
	 */
	public static void displayTable(RandomAccessFile ref) {
		// ArrayList to hold the list of cell numbers/pointers of each record
		ArrayList<Integer> list = new ArrayList<>();
		DavisBaseUtils.extractCellOffsets(list, ref);
		
		/*
		 * Traverse through the list and deal with each record
		 * Terminology : Record is a Pay load 
		 * 				 Cell is a (Pay load + Pay load header(rowid+payloadSize))
		 */
		try {
			for (int i=0; i<list.size(); i++) {
				int itemOffset = 7;
				int itemSizeOffset = 7;
				Formatter f = new Formatter();		// create a Formatter to format the output in proper alignment
			
				ref.seek(list.get(i));				// Set file pointer within the file so the starting byte of cell/record
				ref.skipBytes(2);					// skip first two bytes which represent the payload size

				int rowId = ref.readInt();			// read the rowID
				f.format("%d", rowId);				// put the rowId in the formatter
				int numOfColumns = ref.readByte();	// read number of columns
				itemOffset = itemOffset + (int) numOfColumns;	// itemOffset is now pointing to the first byte of payload
			
				for(int j = 0; j < numOfColumns; j++) {
					ref.seek(list.get(i) + itemSizeOffset);
					itemSizeOffset = itemSizeOffset + 1;
					int clmSize = (int) ref.readByte();
					if (clmSize == 0) {								// case when the column size is zero
						f.format("%18s", "Null");
					} else if (clmSize >= 12) {						// case when the column size is equal/more than 12 (indicates TEXT)
						clmSize = clmSize - 12;
						byte[] b = new byte[clmSize];				// byte array of size exactly equal to the size of payload
						ref.seek(list.get(i) + itemOffset);
						itemOffset = itemOffset + clmSize;
						ref.readFully(b);							// reads exactly the number of bytes equal to the size of byte array b
						StringBuilder str = new StringBuilder("");
						DavisBaseUtils.appendBytesToStringBuilder(str, b);
						f.format("%18s", str);
					} else if (clmSize == 1) {						// case when the column size is 1. TINYINT
						ref.seek(list.get(i) + itemOffset);
						itemOffset = itemOffset + clmSize;
						int value = (int) ref.readByte();
						f.format("%18d", value);
					} else {
						System.out.println("The bytes doesn't seem to be in TEXT format");
					}
				}
				System.out.println(f);
			}
		} catch (Exception e) {
			out.println("Unable to access the database_table file");
			out.println(e);
		}
	}
}
