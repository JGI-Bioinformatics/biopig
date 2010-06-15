package gov.jgi.meta.hadoop.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReadNode implements Writable, Comparable {
    public String id;
    public String hash;
    public String sequence;

  public ReadNode(String id, String hash, String sequence) {
    this.id = id;
    this.hash = hash;
    this.sequence = sequence;
  }

  public ReadNode(ReadNode n) {
      if (n != null) {
      this.id = n.id;
      this.hash = n.hash;
      this.sequence = n.sequence;
      }
  }
  public ReadNode() {
    this("", "", "");
  }

    public ReadNode(String serialized) {
        String[] a = serialized.split("&");
        this.id = a[0];
        this.hash = a[1];
        this.sequence = a[2];
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

    public boolean equals(Object o) {
      if (!(o instanceof ReadNode)) {
        return false;
      }
      ReadNode other = (ReadNode) o;
      return (other.id.equals(this.id));
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public int compareTo(Object r) {
        if (!(r instanceof ReadNode)) {
            throw new ClassCastException("object can't be compared, wrong class");
        } else {
            ReadNode rn = (ReadNode) r;
            return this.id.compareTo(rn.id);
        }
    }

}
