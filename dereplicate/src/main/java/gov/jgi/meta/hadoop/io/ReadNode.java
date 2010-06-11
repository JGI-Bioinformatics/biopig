package gov.jgi.meta.hadoop.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReadNode implements Writable {
    public String id;
    public String hash;
    public String sequence;

  public ReadNode(String id, String hash, String sequence) {
    this.id = id;
    this.hash = hash;
    this.sequence = sequence;
  }

  public ReadNode() {
    this("", "", "");
  }

  public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, id);
      WritableUtils.writeString(out, hash);
      WritableUtils.writeString(out, sequence);
  }

  public void readFields(DataInput in) throws IOException {

      id = WritableUtils.readString(in);
      hash = WritableUtils.readString(in);
      sequence = WritableUtils.readString(in);

  }

  public String toString() {
      return id + "&" + hash + "&" + sequence;
  }

}
