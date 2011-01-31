#!/usr/bin/env groovy


HashSet<String> S = new HashSet<String>();

        private String stringReplaceIth(String s, int i, char c) {

            return s.substring(0,i) + c + s.substring(i+1);

        }

String modifySequence(String seq, int editDistance, int width)
{
      String modifiedSeq = new String(seq);
      int pos;

   while (editDistance-- > 0) {

     pos = Math.round(Math.random() * 1000) % width;

      if (seq.getAt(pos) == "A") {
        modifiedSeq =  stringReplaceIth(modifiedSeq, pos, 'G' as char);
      } else {
        modifiedSeq =  stringReplaceIth(modifiedSeq, pos, 'A' as char);
      }
  }
  return modifiedSeq;
}

System.in.eachLine() { line ->

   if (line.charAt(0) != '>') {
       S.add(line);
   }
}

for (int i = 0; i < 10; i++) {
  if (i == 0) {
    for (String seq : S) {
      String name = Math.random();
      println(">" + name + "/1 original")
      println(seq)
      println(">" + name + "/2 original")
      println(seq)
    }
  } else {
    for (String seq : S) {
      //println("seq = " + seq);
      String modifiedSeq = modifySequence(seq, 1, 10);
      //println("mod = " + modifiedSeq);
      String name = Math.random();
      println(">" + name + "/1 editdistance=1")
      println(modifiedSeq)
      println(">" + name + "/2 editdistance=1")
      println(seq)

      modifiedSeq = modifySequence(modifiedSeq, 1, 10);
      //println("mod = " + modifiedSeq);
      name = Math.random();
      println(">" + name + "/1 editdistance=2")
      println(modifiedSeq)
      println(">" + name + "/2 editdistance=2")
      println(seq)
    }
  }
}