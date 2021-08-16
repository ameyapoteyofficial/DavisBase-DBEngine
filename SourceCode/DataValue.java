import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

/*
 * Wrapper class for a data value in a record.
 *
 * If we plan on supporting the DATETIME and DATE data types, I could add getters
 * and setters for those. You would just set them with the string datetime or date
 * value, and the setter function would convert it into the appropriate java
 * data type for storage.
 */
public class DataValue {
  private byte[] value;
  private Integer code;

  public DataValue() { }
  public DataValue(Integer code) {
    this.setCode(code);
  }

  public byte[] getBytes() {
    return this.value;
  }
  public void setBytes(byte[] value) {
    this.value = value;
  }

  // @Override
  // public int compareTo(DataValue dv) {
  //   if (this.code != dv.code) {
  //     throw new RuntimeException("Error: Invalid types in comparison");
  //   }
  //   switch (this.code) {
  //     // TINYINT
  //     case 0x01:
  //       return this.getTinyint().compareTo(dv.getTinyint);
  //     // SMALLINT
  //     case 0x02:
  //       return this.getTinyint().compareTo(dv.getTinyint);
  //     // INT
  //     case 0x03:
  //       return this.getTinyint().compareTo(dv.getTinyint);
  //     // BIGINT
  //     case 0x04:
  //       return this.getTinyint().compareTo(dv.getTinyint);
  //     // FLOAT
  //     case 0x05:
  //       return this.getTinyint().compareTo(dv.getTinyint);
  //     // DOUBLE
  //     case 0x06:
  //       return this.getTinyint().compareTo(dv.getTinyint);
  //   }
  //   int last = this.lastName.compareTo(au.lastName);
  //   return last == 0 ? this.firstName.compareTo(au.firstName) : last;
  // }

  /*
   * Read and write
   */
  public void readFromDisk(RandomAccessFile file) {
    try {
      file.read(this.value);
    } catch (IOException e) {
      System.out.println(e);
    }
  }
  public void writeToDisk(RandomAccessFile file) {
    try {
      file.write(this.value);
    } catch (IOException e) {
      System.out.println(e);
    }
  }

  /*
   * Getters
   */
  public Integer getCode() {
    return this.code;
  }
  public Integer getSize() {
    return this.value.length;
  }
  public byte[] getValue() {
    return this.value;
  }
  public Byte getTinyint() {
    return this.value[0];
  }
  public Short getSmallint() {
    ByteBuffer buffer = ByteBuffer.wrap(this.value);
    return buffer.getShort();
  }
  public Integer getInt() {
    ByteBuffer buffer = ByteBuffer.wrap(this.value);
    return buffer.getInt();
  }
  public Long getBigint() {
    ByteBuffer buffer = ByteBuffer.wrap(this.value);
    return buffer.getLong();
  }
  public Float getFloat() {
    ByteBuffer buffer = ByteBuffer.wrap(this.value);
    return buffer.getFloat();
  }
  public Double getDouble() {
    ByteBuffer buffer = ByteBuffer.wrap(this.value);
    return buffer.getDouble();
  }
  public String getText() {
    return new String(this.value, StandardCharsets.UTF_8);
  }

  /*
   * Setters
   */
  public void setCode(Integer code) {
    Integer size = 0;

    if (code >= DataType.TEXT.code) {
      size = code - DataType.TEXT.code;
      code = DataType.TEXT.code;
    } else {
      size = DataType.getSize(code);
    }
    this.value = new byte[size];
    this.code = code;
  }
  public void setTinyint(Byte value) {
    this.value[0] = value;
  }
  public void setSmallint(Short value) {
    ByteBuffer buffer = ByteBuffer.allocate(2);
    buffer.putShort(value);
    this.value = buffer.array();
  }
  public void setInt(Integer value) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(value);
    this.value = buffer.array();
  }
  public void setBigint(Long value) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(value);
    this.value = buffer.array();
  }
  public void setFloat(Float value) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putFloat(value);
    this.value = buffer.array();
  }
  public void setDouble(Double value) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putDouble(value);
    this.value = buffer.array();
  }
  public void setText(String value) {
    this.value = value.getBytes(StandardCharsets.UTF_8);
  }
}