package gov.jgi.meta.hadoop.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ReadNodeSet implements Writable {
    public int length;
    public Set<ReadNode> s;

  public ReadNodeSet(Set<ReadNode> s) {
    this.s = s;
    length = s.size();
  }

  public ReadNodeSet(Iterable<ReadNode> v) {
      this.s = new TreeSet<ReadNode>();
      Iterator<ReadNode> i;

      i = v.iterator();
      
      while(i.hasNext()) {
          s.add(new ReadNode(i.next()));
      }

      this.length = s.size();

  }


  public ReadNodeSet() {
    this(new TreeSet<ReadNode>());
  }

    public ReadNodeSet(String serialized) {
        this.s = new TreeSet<ReadNode>();

        String[] a = serialized.split(",");
        for (int i = 1; i < a.length; i++) {
            ReadNode r = new ReadNode(a[i]);
            s.add(r);
        }

        this.length = s.size();

    }

  public void write(DataOutput out) throws IOException {

      out.writeInt(length);
      for (ReadNode r : s) {
          r.write(out);
      }
  }

  public void readFields(DataInput in) throws IOException {

      this.length = in.readInt();
      this.s = new TreeSet<ReadNode>();
      for (int i = 0; i < length; i++) {
         ReadNode r = new ReadNode();
         r.readFields(in);
         s.add(r);
      }
  }

  public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("length=" + length);
      for (ReadNode r : this.s) {
          sb.append(",").append(r.toString());
      }
      return sb.toString();
  }


  public String canonicalName() {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (ReadNode r : this.s) {
          if (first) {
              sb.append(r.id);
              first = false;
          }
          else {
              sb.append(",").append(r.id);
          }
      }
      return sb.toString();
  }
}
