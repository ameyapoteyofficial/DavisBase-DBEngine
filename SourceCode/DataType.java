import java.util.HashMap;

public enum DataType {
  NULL(0x00),
  TINYINT(0x01),
  SMALLINT(0x02),
  INT(0x03),
  BIGINT(0x04),
  FLOAT(0x05),
  DOUBLE(0x06),
  YEAR(0x08),
  TIME(0x09),
  DATETIME(0x0A),
  DATE(0x0B),

  // Type code values of the form 0x0C + n are text, where n is the length of
  // the text in characters. This restricts values of TEXT to length 0xFF
  // minus 0x0C.
  TEXT(0x0C);

  private static HashMap<Integer,Integer> SIZES;

  static {
    SIZES = new HashMap<>();
    SIZES.put(NULL.code, 0);
    SIZES.put(TINYINT.code, 1);
    SIZES.put(SMALLINT.code, 2);
    SIZES.put(INT.code, 4);
    SIZES.put(BIGINT.code, 8);
    SIZES.put(FLOAT.code, 4);
    SIZES.put(DOUBLE.code, 8);
    SIZES.put(YEAR.code, 1);
    SIZES.put(TIME.code, 4);
    SIZES.put(DATETIME.code, 8);
    SIZES.put(DATE.code, 8);
    SIZES.put(TEXT.code, 0);
  }
  public Integer code;

  DataType(Integer code) {
    this.code = code;
  }
  public static Integer getSize(Integer code) {
    if (code <= TEXT.code) {
      return SIZES.get(code);
    } else {
      return code - TEXT.code;
    }
  }
  public static Integer getTextCode(Integer size) {
    return DataType.TEXT.code + size;
  }
  
  public static Integer getCodeForDataType(String type) {
	  switch(type) {
	  	case "tinyint":
	  		return TINYINT.code;
	  	case "smallint":
	  		return SMALLINT.code;
	  	case "int":
	  		return INT.code;
	  	case "bigint":
	  		return BIGINT.code;
	  	case "long":
	  		return BIGINT.code;
	  	case "float":
	  		return FLOAT.code;
	  	case "double":
	  		return DOUBLE.code;
	  	case "year":
	  		return YEAR.code;
	  	case "time":
	  		return TIME.code;
	  	case "datetime":
	  		return DATETIME.code;
	  	case "date":
	  		return DATE.code;
	  	case "text":
	  		return TEXT.code;
	  	default:
	  		return NULL.code;
	  }
  }
}