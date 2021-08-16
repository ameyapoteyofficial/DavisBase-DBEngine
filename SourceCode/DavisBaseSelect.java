import static java.lang.System.out;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

public class DavisBaseSelect {
	
	public static void commandUsage() {
		System.out.println("Invalid command");
		System.out.println("USAGE:     SELECT * FROM <table_name>;");
		System.out.println("USAGE:     SELECT * FROM <table_name> WHERE rowid=<rowid>;");
		System.out.println("USAGE:     SELECT * FROM <table_name> WHERE NOT rowid=<rowid>;");
		System.out.println("USAGE:     SELECT <column_name> FROM <table_name> WHERE rowid=<rowid>;");
		System.out.println("USAGE:     SELECT <column_name> FROM <table_name> WHERE NOT rowid=<rowid>;");
	}
	
	public static void parseQuery(String cmdStr) {
		ArrayList<String> cmdTokens = new ArrayList<String>(Arrays.asList(cmdStr.split(" ")));
		if (cmdTokens.size() < 4 || cmdTokens.size() > 7) {
			commandUsage();
			return;
		}
		//if (cmdTokens.get(1).equals("*")) {
		if (cmdTokens.get(2).equals("from")) {
			String tableName = cmdTokens.get(3).trim();
			String tableFileName = tableName + ".tbl";
			if (!DavisBaseUtils.isTablePresent(tableFileName)) {
				System.out.println("Table name does not exists");
			} else {
				if (cmdTokens.size() > 4) {
					if (cmdTokens.get(4).equals("where")) {
						boolean negate = false;
						String rowidStr = "";
						if (cmdTokens.size() == 7) {
							if (cmdTokens.get(5).equals("not")) {
								negate = true;
								rowidStr = cmdTokens.get(6);
							} else {
								commandUsage();
							}
						} else {
							rowidStr = cmdTokens.get(5);
						}
						
						if (rowidStr.contains(">=")) {
							if ((rowidStr.substring(0, rowidStr.indexOf(">="))).equals("rowid")) {
								int rowId = Integer.parseInt(rowidStr.substring(rowidStr.indexOf(">=")+2));
								runQuery(tableName, rowId, ">=", negate, cmdTokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (rowidStr.contains("<=")) {
							if ((rowidStr.substring(0, rowidStr.indexOf("<="))).equals("rowid")) {
								int rowId = Integer.parseInt(rowidStr.substring(rowidStr.indexOf("<=")+2));
								runQuery(tableName, rowId, "<=", negate, cmdTokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (rowidStr.contains("<>")) {
							if ((rowidStr.substring(0, rowidStr.indexOf("<>"))).equals("rowid")) {
								int rowId = Integer.parseInt(rowidStr.substring(rowidStr.indexOf("<>")+2));
								runQuery(tableName, rowId, "<>", negate, cmdTokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (rowidStr.contains(">")) {
							if ((rowidStr.substring(0, rowidStr.indexOf(">"))).equals("rowid")) {
								int rowId = Integer.parseInt(rowidStr.substring(rowidStr.indexOf(">")+1));
								runQuery(tableName, rowId, ">", negate, cmdTokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (rowidStr.contains("<")) {
							if ((rowidStr.substring(0, rowidStr.indexOf("<"))).equals("rowid")) {
								int rowId = Integer.parseInt(rowidStr.substring(rowidStr.indexOf("<")+1));
								runQuery(tableName, rowId, "<", negate, cmdTokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (rowidStr.contains("=")) {
							if ((rowidStr.substring(0, rowidStr.indexOf("="))).equals("rowid")) {
								int rowId = Integer.parseInt(rowidStr.substring(rowidStr.indexOf("=")+1));
								runQuery(tableName, rowId, "=", negate, cmdTokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else {
							commandUsage();
						}
					} else {
						commandUsage();
					}
				} else {
					if (!cmdTokens.get(1).equals("*")) {
						commandUsage();
					} else {
						runQuery(tableName, 0, "", false, cmdTokens.get(1));		// print all records
					}
				}
			}
		} else {
			commandUsage();
		}
	}
	
	public static void runQuery(String tableNm, int rowId, String oper, boolean negate, String columnNm) {
		boolean found = false;
		try {
			RandomAccessFile queryTable = new RandomAccessFile("data/" + tableNm + ".tbl", "rw");
			ArrayList<Integer> list = new ArrayList<>();
			DavisBaseUtils.extractCellOffsets(list, queryTable);
			//System.out.println("Size of list: " + list.size());
			HashMap<String, String> columnToType = new HashMap<String, String>();
			HashMap<Integer, String> columnToOrdi = new HashMap<Integer, String>();
			HashMap<String, String> columnToIsNull = new HashMap<String, String>();
			DavisBaseUtils.extractColmnNames(tableNm, columnToOrdi, columnToType, columnToIsNull);
			if (columnNm.equals("*")) {
				DavisBaseUtils.printTableHeader(columnToOrdi);
			}
					
			for (int i=0; i<list.size(); i++) {
				queryTable.seek(list.get(i)+2);		// Set file pointer within the file so the starting byte of cell/record
				int id = queryTable.readInt();
				if (oper.equals("")) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("=") && !negate && (id == rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
					break;
				} else if (oper.equals("=") && negate && (id != rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals(">") && !negate && (id > rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals(">") && negate && (id <= rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("<") && !negate && (id < rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("<") && negate && (id >= rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals(">=") && !negate && (id >= rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals(">=") && negate && (id < rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("<=") && !negate && (id <= rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("<=") && negate && (id > rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("<>") && !negate && (id != rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				} else if (oper.equals("<>") && negate && (id == rowId)) {
					if (!extractValues(columnToOrdi, columnToType, queryTable, list.get(i), columnNm)) {
						System.out.println("Invalid column name");
						found = true;
						break;
					}
					found = true;
				}
			}
			if (!found) {
				System.out.println("Item not found");
			}
			queryTable.close();
			System.out.println();
		} catch (Exception e) {
			out.println("Unable to get the values from the DB tables");
			out.println(e);
		}
	}

	public static boolean extractValues(HashMap<Integer, String> columnToOrdi,
									 HashMap<String, String> columnToType, 
									 RandomAccessFile ref, Integer offSet, String columnNm) {
		boolean found = false;
		try {
			int itemOffset = offSet + 7;
			int itemSizeOffset = offSet + 7;
			Formatter f = new Formatter();			// create a Formatter to format the output in proper alignment
			
			ref.seek(offSet+2);
			int rowId = ref.readInt();
			f.format("%d", rowId);
			int numOfColumns = ref.readByte();
			itemOffset = itemOffset + numOfColumns;
			
			for(int i = 0; i < numOfColumns; i++) {
				String clmName = columnToOrdi.get(i);
				String clmType = columnToType.get(clmName);
				if (!columnNm.equals("*") && !columnNm.equals(clmName)) {
					ref.seek(itemSizeOffset);
					int code = (int) ref.readByte();
					itemSizeOffset = itemSizeOffset + 1;
					int elemSize = DataType.getSize(code);
					itemOffset = itemOffset + elemSize;
					continue;
				}
				found = true;
				ref.seek(itemSizeOffset);
				int code = (int) ref.readByte();
				itemSizeOffset = itemSizeOffset + 1;
				int elemSize = DataType.getSize(code);
				if (elemSize == 0) {
					f.format("%18s", "Null");
				} else {
					byte[] b = new byte[elemSize];
					ref.seek(itemOffset);
					itemOffset = itemOffset + elemSize;
					ref.readFully(b);

					if (clmType.equals("tinyint")) {
						f.format("%18d", (int) b[0]);
					} else if (clmType.equals("smallint")) {
						f.format("%18d", ByteBuffer.wrap(b).getShort());
					} else if (clmType.equals("int")) {
						f.format("%18d", ByteBuffer.wrap(b).getInt());
					} else if (clmType.equals("bigint")) {
						f.format("%18d", ByteBuffer.wrap(b).getLong());
					} else if (clmType.equals("long")) {
						f.format("%18d", ByteBuffer.wrap(b).getLong());
					} else if (clmType.equals("float")) {
						f.format("%18f", ByteBuffer.wrap(b).getFloat());
					} else if (clmType.equals("double")) {
						f.format("%18f", ByteBuffer.wrap(b).getDouble());
					} else if (clmType.equals("year")) {
						f.format("%18d", ByteBuffer.wrap(b).getShort());
					} else if (clmType.equals("time")) {
						f.format("%18s", DavisBaseParser.unparseTime(b));
					} else if (clmType.equals("datetime")) {
						f.format("%18s", DavisBaseParser.unparseDateTime(b));
					} else if (clmType.equals("date")) {
						f.format("%18s", DavisBaseParser.unparseDate(b));
					} else if (clmType.equals("text")) {
						String strVal = new String(b);
						f.format("%18s", strVal);
					} else {
						System.out.println("Type not recognized, printing in bytes");
						f.format("%18s", b);
					}
				}
			}
			if (found) {
				System.out.println(f);
			}
		} catch (Exception e) {
			out.println("Unable to extract values from database_table files");
			out.println(e);
		}
		return found;
	}
}
