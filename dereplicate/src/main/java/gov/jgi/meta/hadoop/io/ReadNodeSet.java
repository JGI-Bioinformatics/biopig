package gov.jgi.meta.hadoop.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ReadNodeSet implements WritableComparable {
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

    public int compareTo(Object r) {
        if (!(r instanceof ReadNodeSet)) {
            throw new ClassCastException("object can't be compared, wrong class");
        } else {
            ReadNodeSet rns = (ReadNodeSet) r;
            return this.canonicalName().compareTo(rns.canonicalName());
        }
    }

    public String fastaHeader() {
        return ">" + this.canonicalName();
    }

    public String fastaConsensusSequence() {

        String[] bases = {"a", "t", "g", "c"};
        int[] totals = {0, 0, 0, 0};  // a t g c
        StringBuilder sb = new StringBuilder();
        sb.append("\n");

        // just take majority value at each position
        ReadNode[] rnArray = (ReadNode[]) s.toArray(new ReadNode[s.size()]);

        if (length == 1) return "\n" + rnArray[0].sequence;
        else {
            int seqLength = rnArray[0].sequence.length();
            for (int i = 0; i < seqLength; i++) {
                totals[0] = 0; totals[1] = 0; totals[2] = 0; totals[3] = 0;
                for (int j = 0; j < rnArray.length; j++) {
                    int[] count = rnArray[j].getCounts(i);
                    for (int k = 0; k < 4; k++) {
                        totals[k] += count[k];
                    }
                }

                int max = 0;
                int maxk = 0;
                for (int k = 0; k < 4; k++) {
                    if (totals[k] > max) {
                        max = totals[k];
                        maxk = k;
                    }
                }
                sb.append(bases[maxk]);
            }
        }
        return sb.toString();
    }

}
