package gov.jgi.meta;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReadNode implements WritableComparable {
    public String id;
    public String hash;
    public String sequence;
    public int count;

  public ReadNode(String id, String hash, String sequence) {
    this.id = id;
    this.hash = hash;
    this.sequence = sequence;
    this.count = 1;
  }

  public ReadNode(ReadNode n) {
      if (n != null) {
      this.id = n.id;
      this.hash = n.hash;
      this.sequence = n.sequence;
      this.count = n.count;
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
        this.count = 1;
    }

  public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, id);
      WritableUtils.writeString(out, hash);
      WritableUtils.writeString(out, sequence);
      WritableUtils.writeVInt(out, count);
  }

  public void readFields(DataInput in) throws IOException {

      id = WritableUtils.readString(in);
      hash = WritableUtils.readString(in);
      sequence = WritableUtils.readString(in);
      count = WritableUtils.readVInt(in);
  }

  public String toString() {
      return id + "&" + hash + "&" + sequence;
  }

    public boolean equals(Object o) {
      if (!(o instanceof ReadNode)) {
        return false;
      }
      ReadNode other = (ReadNode) o;
      return (other.sequence.equals(this.sequence));
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

    public int[] getCounts(int i) {
        int[] totals = {0, 0, 0, 0}; // a t g c
        if (sequence.charAt(i) == 'a') totals[0]++;
        else if (sequence.charAt(i) == 't') totals[1]++;
        else if (sequence.charAt(i) == 'g') totals[2]++;
        else if (sequence.charAt(i) == 'c') totals[3]++;

        return totals;
    }

}