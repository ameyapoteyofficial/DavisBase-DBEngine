import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;

import static java.lang.System.out;

public class DavisBaseCreateTable {

	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 *  
	 *  Sample create command,
	 *  
	 *  CREATE TABLE CONTACT (
  	 * 		Contact_id	INT NOT NULL,
  	 *		Fname	 	TEXT NOT NULL,
  	 *		Mname		TEXT,
  	 *		Lname		TEXT NOT NULL
	 *	);
	 */
	public static void parseCreateTable(String cmdStr) {
		boolean isTypeValid = true;
		ArrayList<String> cmdTokens = new ArrayList<String>(Arrays.asList(cmdStr.split("\s+")));
		
		if (cmdTokens.get(1).equals("table")) {
			//System.out.println("Parsing the string:\"" + cmdStr + "\"");
			String columns = cmdStr.substring(cmdStr.indexOf('(')+1,  cmdStr.lastIndexOf(')')).trim();
			ArrayList<String> colmTokens = new ArrayList<String>(Arrays.asList(columns.split(",")));
			DavisBaseUtils.trimArrayFields(colmTokens);
			
			for(int i=0; i < colmTokens.size() ; i++) {
				ArrayList<String> colmDetails = 
						new ArrayList<String>(Arrays.asList(colmTokens.get(i).replaceAll("[\t]+", " ").replace("\n", " ").replaceAll("[ ]+", " ").trim().split(" ")));
				if(!isValidDatatype(colmDetails.get(1).toLowerCase())) {
					isTypeValid = false;
				}
				
			}
			
			if(isTypeValid) {
				/* Define table file name */
				String tableName = cmdTokens.get(2).trim();
				String tableFileName = tableName + ".tbl";
			
				if (DavisBaseUtils.isTablePresent(tableFileName)) {
					System.out.println("ERROR: Table name already exists");
				} else {
					createTable(tableFileName);
					/*  Code to insert a row in the davisbase_tables table 
					 *  i.e. database catalog meta-data 
					 */
					addTableName(tableName);
					
					/*  Code to insert rows in the davisbase_columns table  
					 *  for each column in the new table 
					 * 	i.e. database catalog meta-data  colmTokens.size()
				 	*/
					for(int i=0; i < colmTokens.size() ; i++) {
						String columnDetails = colmTokens.get(i).replaceAll("[\t]+", " ").replace("\n", " ").replaceAll("[ ]+", " ").trim();
						addColumnName(columnDetails, tableName, i);
					}
					//DavisBaseUtils.dumpDavisTables();
					System.out.println(tableName + " table created!!");
				}
			} else {
				System.out.println("Cannot Create Table!! invalid Dataype(s)");
			}
		} else if (cmdTokens.get(1).equals("index")) {
			// 	TODO: Index implementation not needed for the project
		} else {
			System.out.println("Invalid command");
		}
	}
	
	public static void createTable(String tableNm) {
		/*  Code to create a .tbl file to contain table data */
		try {
			RandomAccessFile tableFile = new RandomAccessFile("data/" + tableNm, "rw");
			tableFile.setLength(DavisBaseUtils.pageSize);
			tableFile.seek(0);
			tableFile.writeByte(0x0D);
			tableFile.seek(DavisBaseUtils.leafOrNode);
			tableFile.writeShort(65535);
			tableFile.writeShort(65535);
			tableFile.writeShort(65535);
			tableFile.writeShort(65535);
			tableFile.close();
			//out.println("The file is now " + tableFile.length() + " bytes long");
			//out.println("The file is now " + tableFile.length() / pageSize + " pages long");
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	/**
	 * Method to write the table name into the davisbase_tables.tbl file.
	 * This is the first function called from DavisBasePrompt.java when 
	 * new table is created by user.
	 * @param tableNm
	 */
	public static void addTableName(String tableNm) {
		try {
			RandomAccessFile TablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			/**
			 * construct the cell with 
			 * 2 bytes of payLoad size + 4 bytes of rowId + 1 bytes for number of columns + 
			 * 1 bytes for each column size.. + payLoad
			 */
			ByteBuffer payLoadSz = ByteBuffer.allocate(2); 		// 2 bytes for payload size
			payLoadSz.putShort((short) tableNm.length());		// add size of payload (table name length here)
			
			ByteBuffer rowId = ByteBuffer.allocate(4);			// 4 bytes for row id
			int currRowId = DavisBaseUtils.getCurrentRowId(TablesCatalog); 
			rowId.putInt((int) ++currRowId);
			
			byte[] recordIdx = new byte[2];						// 1 bytes for number of columns and 1 for its size
			Array.setByte(recordIdx, 0, (byte) 1);				// There is only one column in davisbase_tables
			Array.setByte(recordIdx, 1, (byte) (12 + tableNm.length()));	// size of payload is size of table name + 12 (to indicate that the payload is of type TEXT)

			byte [] payLoad = tableNm.getBytes();
			
			int cellSize = payLoadSz.array().length + rowId.array().length + recordIdx.length + payLoad.length;  // total size of the cell

			List<Byte> arr = new ArrayList<Byte>();				// ArrayList of bytes to append all the above
			byte[] sz = payLoadSz.array();						// returns array of bytes
			byte[] rid = rowId.array();
			
			DavisBaseUtils.appendBytesToList(arr, sz);			// Append bytes to list
			DavisBaseUtils.appendBytesToList(arr, rid);
			DavisBaseUtils.appendBytesToList(arr, recordIdx);
			DavisBaseUtils.appendBytesToList(arr, payLoad);
			
			byte[] cellBytes = new byte[arr.size()];				// construct array of bytes
			DavisBaseUtils.convertListToByteArray(cellBytes, arr);	// copy bytes from list to the array of bytes
			/* Finished cell construction */

			DavisBaseUtils.insertCellBytesIntoPage(TablesCatalog, cellBytes, cellSize);
			TablesCatalog.close();									// Mandatory need for the close of file
		}
		catch (Exception e) {
			out.println("Unable to add table names in davisbase_tables.tbl file");
			out.println(e);
		}
	}
	
	
	/**
	 * Method to write the table name and column names into the davisbase_columns.tbl file.
	 * This is the second function called after writeTableName from DavisBasePrompt.java when 
	 * new table is created by user.
	 * @param arr
	 * @param tableNm
	 * @param pos
	 */
	public static void addColumnName(String columnDetails, String tableNm, Integer pos) {
		boolean isNull = true;
		if (columnDetails.contains("not null")) {
			isNull = false;
		}
		ArrayList<String> colmDetailsList = new ArrayList<String>(Arrays.asList(columnDetails.split(" ")));
		try {
			RandomAccessFile ColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			/**
			 * construct the cell with payLoad size + rowId + payLoad
			 */
			ByteBuffer payLoadSz = ByteBuffer.allocate(2);		// 2 bytes for payload size
			
			ByteBuffer rowId = ByteBuffer.allocate(4);			// 4 bytes for row id
			int currRowId = DavisBaseUtils.getCurrentRowId(ColumnsCatalog);
			rowId.putInt((int) ++currRowId);
			
			byte[] recordIdx = new byte[6];						// one byte to store number of columns and rest to store size of each column value
			/*
			 * one byte value representing the number of columns. 
			 * Number of columns here are 5 in davisbase_columns.tbl  (table name, column name, data_type, ordinal pos, is_nullable)
			 */
			Array.setByte(recordIdx, 0, (byte) 5);
			Array.setByte(recordIdx, 1, (byte) (12 + tableNm.length()));				// one byte value representing table name length in the record body
			Array.setByte(recordIdx, 2, (byte) (12 + colmDetailsList.get(0).length()));	// one byte value representing column name length in the record body
			Array.setByte(recordIdx, 3, (byte) (12 + colmDetailsList.get(1).length())); // one byte value representing data type length of column in the record body
			Array.setByte(recordIdx, 4, (byte) 1);										// one byte value representing the ordinal position of column
			if (isNull) {
				Array.setByte(recordIdx, 5, (byte) (12 + 3)); 	// one byte value representing the size of fifth element in record body (is nullable) YES
			} else {
				Array.setByte(recordIdx, 5, (byte) (12 + 2)); 	// one byte value representing the size of fifth element in record body (is nullable) NO
			}
			
			List<Byte> payLoad = new ArrayList<Byte>();
			byte[] b1 = tableNm.getBytes();						
			DavisBaseUtils.appendBytesToList(payLoad, b1);		// table name goes into payload as first entry
			byte[] b2 = colmDetailsList.get(0).getBytes();		
			DavisBaseUtils.appendBytesToList(payLoad, b2);		// column name goes into payload as second entry
			byte[] b3 = colmDetailsList.get(1).getBytes();
			DavisBaseUtils.appendBytesToList(payLoad, b3);		// column data type goes into payload as third entry
			byte b4 = pos.byteValue();
			payLoad.add(b4);									// ordinal position as forth entry
			String nullAble = "NO";
			if (isNull) {
				nullAble = "YES";
			}
			byte[] b5 = nullAble.getBytes();					// "YES" or "NO" goes into payload as fifth entry
			DavisBaseUtils.appendBytesToList(payLoad, b5);  

			payLoadSz.putShort((short) payLoad.size());			// put the complete size of payload in to payLoadSz (first two bytes of cell)
			
			// Calculate the total cell size
			int cellSize = payLoadSz.array().length + rowId.array().length
						   + recordIdx.length + payLoad.size();
			
			List<Byte> cellArr = new ArrayList<Byte>();
			byte[] sz = payLoadSz.array();
			byte[] rid = rowId.array();
			
			DavisBaseUtils.appendBytesToList(cellArr, sz);
			DavisBaseUtils.appendBytesToList(cellArr, rid);
			DavisBaseUtils.appendBytesToList(cellArr, recordIdx);
			cellArr.addAll(payLoad);
			
			byte[] cellBytes = new byte[cellArr.size()];
			DavisBaseUtils.convertListToByteArray(cellBytes, cellArr);
			/* Finished cell construction */
	
			DavisBaseUtils.insertCellBytesIntoPage(ColumnsCatalog, cellBytes, cellSize);
			ColumnsCatalog.close();
		} catch (Exception e) {
			out.println("Unable to add columns names in davisbase_columns.tbl file");
			out.println(e);
		}
	}
	
	public static int getPageSize() {
		return DavisBaseUtils.pageSize;
	}
	
	public static boolean isValidDatatype(String check) {
        switch (check) {
            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
            case "long":
            case "float":
            case "double":
            case "year":
            case "time":
            case "datetime":
            case "date":
            case "text":
                return true;
            default:
                return false; 
        }
    }
}