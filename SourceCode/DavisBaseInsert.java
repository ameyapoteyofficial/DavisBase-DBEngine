import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

public class DavisBaseInsert {
	public static void parseInsert(String cmdStr) {
		ArrayList<String> cmdTokens = new ArrayList<String>(Arrays.asList(cmdStr.split("\s+")));
		if (cmdTokens.size() != 7) {
			System.out.println("Error: Invalid insert command");
			System.out.println("USAGE: INSERT INTO TABLE (<column_list>) <table_name> VALUES (<value1, value2...>);");
			return;
		}
		/*
		 * TODO: Prohibit the insert command if primary key is not present in the column_list
		 * TODO: Prohibit the insert command if column name that is set to NOT NULL is not present in the column list
		 */
		String tableName = cmdTokens.get(4).trim();
		String tableFileName = tableName + ".tbl";
		
		if (!DavisBaseUtils.isTablePresent(tableFileName)) {
			System.out.println("This table does not exists in the DB");
		} else {
			/*
			 * Get column names
			 */
			String columnNames = cmdTokens.get(3).trim();
			String columnsStr = columnNames.substring(columnNames.indexOf('(')+1,  columnNames.lastIndexOf(')')).trim();
			ArrayList<String> columnNmTokens = new ArrayList<String>(Arrays.asList(columnsStr.split(",")));
			DavisBaseUtils.trimArrayFields(columnNmTokens);
			//DavisBaseUtils.printArrayFields(columnNmTokens);
			/*
			 * Get column values
			 */
			String columnValues = cmdTokens.get(6).trim();
			String columnValStr = columnValues.substring(columnValues.indexOf('(')+1,  columnValues.lastIndexOf(')')).trim();
			ArrayList<String> columnValTokens = new ArrayList<String>(Arrays.asList(columnValStr.split(",")));
			DavisBaseUtils.trimArrayFields(columnValTokens);
			DavisBaseUtils.removeQuotes(columnValTokens);			// Assumption: user provided values are converted to lower case
			//DavisBaseUtils.printArrayFields(columnValTokens);
			
			HashMap<String, String> map = new HashMap<String, String>();
			for (int i = 0; i < columnNmTokens.size(); i++) {
				map.put(columnNmTokens.get(i), columnValTokens.get(i));
			}
			/*
			for(Map.Entry m: map.entrySet()) {    
				System.out.println(m.getKey()+" "+m.getValue());    
			}
			*/
			insert(map, tableName);
		}
	}
	
	public static void insert(HashMap<String, String> map, String tableNm) {
		try {
			RandomAccessFile insrtTable = new RandomAccessFile("data/" + tableNm + ".tbl", "rw");
			HashMap<String, String> map_columnNmToType = new HashMap<String, String>();
			HashMap<Integer, String> map_ordiPosToColumnNm = new HashMap<Integer, String>();
			HashMap<String, String> map_columnNmToNullable = new HashMap<String, String>();
			DavisBaseUtils.extractColmnNames(tableNm, map_ordiPosToColumnNm, 
											map_columnNmToType, map_columnNmToNullable);
			
			ByteBuffer payLoadSz = ByteBuffer.allocate(2);
			ByteBuffer rowId = ByteBuffer.allocate(4);
			int currRowId = DavisBaseUtils.getCurrentRowId(insrtTable);
			rowId.putInt((int) ++currRowId);
			
			byte[] recordIdx = new byte[map_ordiPosToColumnNm.size()+1];	// one byte to store number of columns and rest to store size of each column value
			Array.setByte(recordIdx, 0, (byte) map_ordiPosToColumnNm.size());
			List<Byte> payLoad = new ArrayList<Byte>();
			
			for(int i = 1; i <= map_ordiPosToColumnNm.size(); i++) {
				String clmName = map_ordiPosToColumnNm.get(i-1);
				String clmType = map_columnNmToType.get(clmName);
				if (map.containsKey(clmName)) {
					//System.out.println("User provided value for the column : " + clmName);
					String value = map.get(clmName);
					List<Byte> ValueInBytes = new ArrayList<Byte>();
					DavisBaseParser.valueToBytes(value, ValueInBytes, clmType);
					int code = 0;
					if (clmType.equals("text")) {
						code = DataType.getCodeForDataType(clmType) + ValueInBytes.size();
					} else {
						code = DataType.getCodeForDataType(clmType);
					}
					Array.setByte(recordIdx, i, (byte) code);
					payLoad.addAll(ValueInBytes);
					ValueInBytes.clear();
				} else {
					//System.out.println("User did not provided value for the column : " + clmName);
					if (map_columnNmToNullable.get(clmName).equals("YES")) {
						Array.setByte(recordIdx, i, (byte) 0);
					} else {
						System.out.println(clmName + " column cannot be null");
						return;
					}
				}
			}

			// Merge all the bytes to construct the cell
			payLoadSz.putShort((short) payLoad.size());
			
			int cellSize = payLoadSz.array().length + rowId.array().length + recordIdx.length + payLoad.size();
			List<Byte> arr = new ArrayList<Byte>();
			byte[] sz = payLoadSz.array();
			byte[] rid = rowId.array();
			DavisBaseUtils.appendBytesToList(arr, sz);
			DavisBaseUtils.appendBytesToList(arr, rid);
			DavisBaseUtils.appendBytesToList(arr, recordIdx);
			arr.addAll(payLoad);

			byte[] cellBytes = new byte[arr.size()];
			DavisBaseUtils.convertListToByteArray(cellBytes, arr);
			
			DavisBaseUtils.insertCellBytesIntoPage(insrtTable, cellBytes, cellSize);
			System.out.println("Record Inserted!!");
			insrtTable.close();
		} catch (Exception e) {
			System.out.println("Unable to insert record to the database_tables file");
			System.out.println(e);
		}
	}
}
