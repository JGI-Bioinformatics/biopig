/*
 * Copyright (c) 2010, The Regents of the University of California, through Lawrence Berkeley
 * National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * (1) Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *
 * (2) Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * (3) Neither the name of the University of California, Lawrence Berkeley National Laboratory, U.S. Dept.
 * of Energy, nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the
 * features, functionality or performance of the source code ("Enhancements") to anyone; however,
 * if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley
 * National Laboratory, without imposing a separate written license agreement for such Enhancements,
 * then you hereby grant the following license: a  non-exclusive, royalty-free perpetual license to install,
 * use, modify, prepare derivative works, incorporate into other computer software, distribute, and
 * sublicense such enhancements or derivative works thereof, in binary and source code form.
 */

package gov.jgi.meta.hadoop.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ReadNodeSet implements WritableComparable {
   public int           length;
   public Set<ReadNode> s;

   public ReadNodeSet(Set<ReadNode> s)
   {
      this.s = new HashSet<ReadNode>(s);
      length = s.size();
   }


   public ReadNodeSet(Iterable<ReadNode> v)
   {
      this.s = new HashSet<ReadNode>();
      Iterator<ReadNode> i;

      i = v.iterator();

      while (i.hasNext())
      {
         s.add(new ReadNode(i.next()));
      }

      this.length = s.size();
   }


   public ReadNodeSet()
   {
      this(new HashSet<ReadNode>());
   }


   public ReadNodeSet(String serialized)
   {
      this.s = new HashSet<ReadNode>();

      String[] a = serialized.split(",");
      for (int i = 1; i < a.length; i++)
      {
         ReadNode r = new ReadNode(a[i]);
         s.add(r);
      }

      this.length = s.size();
   }


   public void write(DataOutput out) throws IOException
   {
      out.writeInt(length);
      for (ReadNode r : s)
      {
         r.write(out);
      }
   }


   public void readFields(DataInput in) throws IOException
   {
      this.length = in.readInt();
      this.s      = new HashSet<ReadNode>();
      for (int i = 0; i < length; i++)
      {
         ReadNode r = new ReadNode();
         r.readFields(in);
         s.add(r);
      }
   }


   public String toString()
   {
      StringBuilder sb = new StringBuilder();

      sb.append("length=" + length);
      for (ReadNode r : this.s)
      {
         sb.append(",").append(r.toString());
      }
      return(sb.toString());
   }


   public boolean findHash(String hash)
   {
      for (ReadNode r : this.s)
      {
         if (hash.equals(r.hash))
         {
            return(true);
         }
      }
      return(false);
   }


   public String canonicalName()
   {
      int totalhashcode = 0;
      //StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (ReadNode r : this.s)
      {
         if (first)
         {
            //      sb.append(r.id);
            totalhashcode += r.id.hashCode();
            first          = false;
         }
         else
         {
            //sb.append(",").append(r.id);
            totalhashcode += r.id.hashCode();
         }
      }
      //return length + "." + sb.toString().hashCode();
      return(length + "." + totalhashcode);
   }


   public String canonicalName(String hash)
   {
      int totalhashcode = 0;
      //StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (ReadNode r : this.s)
      {
         if (first)
         {
            //      sb.append(r.id);
            totalhashcode += r.id.hashCode();
            first          = false;
         }
         else
         {
            //sb.append(",").append(r.id);
            totalhashcode += r.id.hashCode();
         }
      }
      //return length + "." + sb.toString().hashCode();
      return(length + "." + totalhashcode + "." + hash);
   }


   public int compareTo(Object r)
   {
      if (!(r instanceof ReadNodeSet))
      {
         throw new ClassCastException("object can't be compared, wrong class");
      }
      else
      {
         ReadNodeSet rns = (ReadNodeSet)r;
         return(this.canonicalName().compareTo(rns.canonicalName()));
      }
   }


   public String fastaHeader()
   {
      return(">" + this.canonicalName());
   }


   public String fastaConsensusSequence()
   {
      String[] bases = { "a", "t", "g", "c" };
      int[] totals   = { 0, 0, 0, 0 }; // a t g c
      StringBuilder sb = new StringBuilder();
      sb.append("\n");

      // just take majority value at each position
      ReadNode[] rnArray = (ReadNode[])s.toArray(new ReadNode[s.size()]);

      if (length == 1) { return("\n" + rnArray[0].sequence); }
      else
      {
         int seqLength = rnArray[0].sequence.length();
         for (int i = 0; i < seqLength; i++)
         {
            totals[0] = 0;
            totals[1] = 0;
            totals[2] = 0;
            totals[3] = 0;
            for (int j = 0; j < rnArray.length; j++)
            {
               int[] count = rnArray[j].getCounts(i);
               for (int k = 0; k < 4; k++)
               {
                  totals[k] += count[k];
               }
            }

            int max  = 0;
            int maxk = 0;
            for (int k = 0; k < 4; k++)
            {
               if (totals[k] > max)
               {
                  max  = totals[k];
                  maxk = k;
               }
            }
            sb.append(bases[maxk]);
         }
      }
      return(sb.toString());
   }


   public static void main(String[] args) throws Exception
   {
      // test main


      Set<ReadNode> s = new HashSet<ReadNode>();

      int max = Integer.parseInt(args[0]);
      for (int i = 0; i < max; i++)
      {
         s.add(new ReadNode("node-" + i, "abcabcabaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "abcaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
      }
      ReadNodeSet rns       = new ReadNodeSet(s);
      final long  startTime = System.nanoTime();
      final long  endTime;
      try {
         System.out.println("cannonicalname = " + rns.canonicalName());
      }
      finally {
         endTime = System.nanoTime();
      }
      final long duration = endTime - startTime;
      System.out.println("Time = " + duration);
   }
}
