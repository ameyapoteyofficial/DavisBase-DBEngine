import static java.lang.System.out;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class DavisBaseUtils {
	
	/* This static variable controls page size. */
	static int pageSizePower = 9;
	/* This strategy insures that the page size is always a power of 2. */
	static int pageSize = (int)Math.pow(2, pageSizePower);
	
	static short numOfTableRecords  = 0;
	static short numOfcolmRecords	= 0;
	static int cellHeaderBytes 		= 6;
	static int recordCountPointer 	= 2;
	static short mostRecentRecord 	= 4;
	static int leafOrNode 			= 6;
	static int parentPage 			= 10;
	static int startOfcellIndex		= 16;
	
	public static boolean isTablePresent(String tableNm) {
		boolean retVal = false;
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
		
			for (int idx = 0; idx < oldTableFiles.length; idx++) {
				if(oldTableFiles[idx].equals(tableNm)) {
					retVal =  true;
					break;
				}
			}
		} catch (SecurityException se) {
			out.println("Unable to check if table name is already present in the directory");
			out.println(se);
		}
		return retVal;
	}
	
	
	/**
	 * Method to extract all the record cells starting from byte location 17 (after 16 bytes of header)
	 */
	public static void extractCellOffsets(ArrayList<Integer> lst, RandomAccessFile ref) {
		try {
			Integer pageOffSet = 0;
			do {
				ref.seek(pageOffSet + recordCountPointer);
				int recordCount = ref.readShort();
				ref.seek(pageOffSet + startOfcellIndex);
				for (int i = 0; i < recordCount; i++) {
					lst.add((int) ref.readShort());
				}
				ref.seek(pageOffSet + leafOrNode);
				pageOffSet = ref.readInt();
				//System.out.println("Next Page offset: " + pageOffSet);
			} while (pageOffSet != 0xFFFFFFFF);
		} catch (Exception e) {
			out.println("Unable to extract cell offset from the DB table");
			out.println(e);
		}
	}

	public static void trimArrayFields(ArrayList<String> arr) {
		for (int i = 0; i < arr.size(); i++) {
			arr.set(i, arr.get(i).trim());
		}
	}
	
	public static void removeQuotes(ArrayList<String> arr) {
		for (int i = 0; i < arr.size(); i++) {
			String str = arr.get(i);
			if (str.charAt(0) == '\'') {
				arr.set(i, str.substring(1, str.length()-1));
			}
		}
	}
	
	public static void printArrayFields(ArrayList<String> arr) {
		for (int i = 0; i < arr.size(); i++) {
			System.out.println(arr.get(i));
		}
	}
	
	public static Integer getLastCellIndex(int pageOffset, RandomAccessFile ref) {
		int recordCount = 0;
		try {
			ref.seek(pageOffset + recordCountPointer);
			recordCount = ref.readShort();
		} catch (Exception e) {
			out.println("Unable to get the last cell index from the DB table");
			out.println(e);
		}
		return (pageOffset + startOfcellIndex + (recordCount * 2));
	}
	
	/**
	 * Method to increment the number of records in the page header at byte position 3 to 4
	 * This should happen every time a new record is added into the page
	 * @param ref
	 * @param recCount
	 */
	public static void incrementRecordCount(int pageOffset, RandomAccessFile ref) {
		try {
			ref.seek(pageOffset + recordCountPointer);
			int recordCount = ref.readShort();
			ref.seek(pageOffset + recordCountPointer);
			ref.writeShort(++recordCount);
		} catch (Exception e) {
			out.println("Unable to increment the cell count in the DB table");
			out.println(e);
		}
	}
	
	public static int getCurrentRowId(RandomAccessFile ref) {
		int currRecordCount = 0;
		try {
			int pageOffset = getLastPageOffset(ref);
			ref.seek(pageOffset + mostRecentRecord);
			short stPos = ref.readShort();
			ref.seek(stPos+2);
			currRecordCount = ref.readInt();
		} catch (Exception e) {
			out.println("Unable to get the current rowId from the DB table");
			out.println(e);
		}
		return currRecordCount;
	}
	
	public static void appendBytesToList(List<Byte> arr, byte[] b) {
		for (int n=0; n < b.length; n++) {
			arr.add(b[n]);
		}
	}
	
	public static void appendBytesToStringBuilder(StringBuilder str, byte[]b) {
		for (int n=0; n < b.length; n++) {
			str.append((char)b[n]);
		}
	}
	public static void writeCellRecord(byte[] b, int cellIndexPos, short pos, RandomAccessFile ref ) {
		try {
			ref.seek(pos);
			ref.write(b);
			DavisBaseUtils.updateTableCellInfo(cellIndexPos, pos, ref);
		} catch (Exception e) {
			out.println("Unable to write the payload/record body in the DB table");
			out.println(e);
		}
	}
	
	public static void convertListToByteArray(byte[] b, List<Byte> cellBytes) {
		for (int i = 0; i < cellBytes.size(); i++) {
			b[i] = cellBytes.get(i);
		}
	}
	
	public static void insertCellBytesIntoPage(RandomAccessFile ref, byte[] cellBytes, int cellSize) {
		/**
		 * Find the last page of davisbase_tables.tbl file, and
		 * calculate the offset of page to insert the cell constructed above
		 */
		int pageOffset = DavisBaseUtils.getLastPageOffset(ref);
		short newCellOffset = DavisBaseUtils.calcCellPosition(pageOffset, cellSize, ref);
		Integer lastCellIndex = DavisBaseUtils.getLastCellIndex(pageOffset, ref);   // Offset location of last cell in the page
		if (DavisBaseUtils.doesPageOverflow(newCellOffset, lastCellIndex)) {				  // page overflows if newCellOffset < lastCellIndex
			pageOffset = DavisBaseUtils.createNewPage(ref);
			lastCellIndex = pageOffset + 16;
			// System.out.println("Last page offset: " + pageOffset);
			newCellOffset = DavisBaseUtils.calcCellPosition(pageOffset, cellSize, ref);	// calculate offset on new page
		}
		DavisBaseUtils.writeCellRecord(cellBytes, lastCellIndex, newCellOffset, ref);		// write cellBytes (array of bytes) to the page
		DavisBaseUtils.writeCellOffset(pageOffset, newCellOffset, ref);					// modify the most recent offset of cell in the page header (bytes 4 to 5)
		DavisBaseUtils.incrementRecordCount(pageOffset, ref);								// increment the number of records of current page
	}
	
	/**
	 * Method that calculates the cell position from where the record should be inserted.
	 * NOTE: records should be inserted from the bottom of the page 
	 * @param payLoadSz
	 * @param ref
	 * @return
	 */
	public static short calcCellPosition(int lastPageOffset, int payLoadSz, RandomAccessFile ref) {
		short newPos = 0;
		try {
			/*
			 * Calculation should be based on the position of most recent record
			 * inserted. Most recent record start position can be found in the page 
			 * header at byte location 4 (starting from 0).
			 */
			ref.seek(lastPageOffset + mostRecentRecord);
			short stPos = ref.readShort();									// File pointer moves to the next byte
			ref.seek(lastPageOffset + mostRecentRecord);			// Bring back the file pointer
			//System.out.println("Current start position: " + stPos);
			if (stPos == 0) {
				/* stPos = 0 indicates no record is present yet and this is going to
				 * first record
				 */
				newPos = (short) (ref.length() - (payLoadSz));
			} else {
				/* stPos != 0 indicates that this is not the first record
				 */
				newPos = (short) (stPos - (payLoadSz));
			}
			//System.out.println("new start position: " + newPos);
		} catch (Exception e) {
			out.println("Unable to calculate the cell position of the DB table");
			out.println(e);
		}
		return newPos;
	}
	
	/**
	 * Method to update the page offset location of each data cell after 16 bytes of page header
	 * An array of 2-byte integers that indicate the page offset location of each data cell
	 * @param pos
	 * @param ref
	 */
	public static void updateTableCellInfo(int cellIndex, short pos, RandomAccessFile ref) {
		try {
			ref.seek(cellIndex);
			ref.writeShort(pos);
		} catch (Exception e) {
			out.println("Unable to update table cell info in the DB table");
			out.println(e);
		}
	}
	
	public static boolean doesPageOverflow(short newOffset, int existingCellCount) {
		boolean returnVal = false;
		if (newOffset <  existingCellCount) {
			//System.out.println("page overflow");
			returnVal = true;
		}
		return returnVal;
	}
	
	public static Integer createNewPage(RandomAccessFile ref) {
		Integer newPageOffset = 0;
		Integer prevPageOffset = 0;
		try {
			int numOfPages = (int) (ref.length()/pageSize);
			ref.setLength((numOfPages + 1) * pageSize);
			prevPageOffset = (int) (ref.length() - (pageSize*2));
			newPageOffset = (int) (ref.length() - pageSize);
			
			ref.seek(prevPageOffset);
			ref.writeByte(0x05);	// First byte of previous page set to 0x05
			ref.seek(prevPageOffset + leafOrNode);
			ref.writeInt(newPageOffset);
			ref.seek(newPageOffset);
			ref.writeByte(0x0D);
			ref.seek(newPageOffset + leafOrNode);
			ref.writeShort(65535);
			ref.writeShort(65535);
			ref.writeInt(prevPageOffset);
		} catch (Exception e) {
			out.println("Error in creating new page in the DB");
			out.println(e);
		}
		return newPageOffset;
	}

	public static void writeCellOffset(int pageOffset, int pos, RandomAccessFile ref) {
		try {
			ref.seek(pageOffset + mostRecentRecord);
			ref.writeShort(pos);
		} catch (Exception e) {
			out.println("Error in writing cell offset in the DB table");
			out.println(e);
		}
	}
	
	public static Integer getLastPageOffset(RandomAccessFile ref) {
		Integer pageOffSet = 0;
		Integer pageType = 0x0D;
		try {
			do {
				ref.seek((long) pageOffSet);
				pageType = (int) ref.readByte();
				if (pageType == 0x05) {
					ref.seek(pageOffSet + leafOrNode);
					pageOffSet = ref.readInt();
				}
			} while (pageType != 0x0D);
		} catch (Exception e) {
			out.println("Error getting the last page offset from the DB table");
			out.println(e);
		}
		return pageOffSet;
	}
	
	public static void extractColmnNames(String cmdTableNm, HashMap<Integer, String> map_ordiPosToClmNm, 
			HashMap<String, String> map_clmNmToType, HashMap<String, String> map_clmNmToIsNull) {
		try {
			RandomAccessFile ClmCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			ArrayList<Integer> list = new ArrayList<>();
			DavisBaseUtils.extractCellOffsets(list, ClmCatalog);
			for (int i = 0; i < list.size(); i++) {
				ClmCatalog.seek(list.get(i) + DavisBaseUtils.cellHeaderBytes + 1);    // +1 to jump to table size byte
				int tableNmLen = ClmCatalog.readByte();
				int colmnNmLen = ClmCatalog.readByte();
				int colmnTypLen = ClmCatalog.readByte();
				int ordiPosLen = ClmCatalog.readByte();
				int nullableLen = ClmCatalog.readByte();
				
				byte[] b = new byte[tableNmLen-12];
				ClmCatalog.readFully(b);						// read the table name in bytes
				StringBuilder tbName = new StringBuilder("");
				appendBytesToStringBuilder(tbName, b);			// convert bytes to string
				
				if (cmdTableNm.equals(tbName.toString())) {
					//	System.out.println("Table Name: " + tbName);
					byte[] b1 = new byte[colmnNmLen-12];
					ClmCatalog.readFully(b1);					// read the column name in bytes
					StringBuilder clmName = new StringBuilder("");
					appendBytesToStringBuilder(clmName, b1);	// convert bytes to string
					
					byte[] b2 = new byte[colmnTypLen-12];
					ClmCatalog.readFully(b2);					// read the column type in bytes
					StringBuilder clmType = new StringBuilder("");
					appendBytesToStringBuilder(clmType, b2);	// convert bytes to string
					
					int ordinalPos = ClmCatalog.readByte();		// read ordinal position
					
					byte[] b3 = new byte[nullableLen-12];
					ClmCatalog.readFully(b3);					// read the if column is nullable
					StringBuilder clmIsNull = new StringBuilder("");
					appendBytesToStringBuilder(clmIsNull, b3);	// convert bytes to string
					
					map_ordiPosToClmNm.put(ordinalPos, clmName.toString());
					map_clmNmToType.put(clmName.toString(), clmType.toString());
					map_clmNmToIsNull.put(clmName.toString(), clmIsNull.toString());
				}
			}
			ClmCatalog.close();
		} catch (Exception e) {
			out.println("Unable to extract column names from davisbase_columns.tbl file");
			out.println(e);
		}
	}
	
	public static void printTableHeader(HashMap<Integer, String> columnToOrdi) {
		Formatter f = new Formatter();			// create a Formatter to format the output in proper alignment
		System.out.println();
		f.format("%s", "rowid");
		for(Entry<Integer, String> m: columnToOrdi.entrySet()) {  
			f.format("%18s", m.getValue());
		}
		System.out.println(f);
		System.out.println("------------------------------------------------------------------------------------------------------");
	}
	
	public static void dumpDavisTables() {
		try {
			RandomAccessFile TablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			RandomAccessFile ColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			System.out.println("Dumping davisbase_tables.tbl");
			DavisBaseHexDump.displayHexDump(TablesCatalog);			// Dump the page
			System.out.println("Dumping davisbase_columns.tbl");
			DavisBaseHexDump.displayHexDump(ColumnsCatalog);
			TablesCatalog.close();
			ColumnsCatalog.close();
		} catch (Exception e) {
			out.println("Unable to hexdump the davisbase tables");
			out.println(e);
		}
	}
}
