#!/usr/bin/env groovy


// stdin is the blast results from geneblaster
// args[1] is the blast results from the commandline

blastresults = args[0];
System.err.println("comparing stdin with " + blastresults);

HashMap<String, Set<String>> s = new HashMap<String, Set<String>>();
HashSet<String> r = new HashSet<String>();

// first read the stdin

int count = 0;
System.in.eachLine() { line ->
//  println("line = " + line);
  larray = line.split("\t", 2);
  gene = larray[0];
  Set<String> seqset = new HashSet<String>();
  seqlist = larray[1];
  for (seq in seqlist.split("\t")) {
//    println("seq = " + seq)
    seqset.add(seq);
    count++;
  }
  s.put(gene, seqset);
  r.addAll(seqset);
}

System.err.println("geneblaster has " + count + " sequence bases, " + r.size() + " unique bases in " + s.keySet().size() + " sets");

// now read the blast results from the file

blastfile = new File(blastresults);

Set<String> g = new HashSet<String>();
Set<String> rr = new HashSet<String>();

int count2 = 0;
blastfile.eachLine { l ->

  count2++;
  
  la = l.split("\t");
  gene = la[0];
  g.add(gene);

  seq = la[1];
  seqBase = seq.split("/")[0];
  rr.add(seqBase);

//  if (s.get(gene)?.contains(seq)) {
//    s.get(gene).remove(seq);
//  } else {
//    println(">> " + gene + "\t" + seq);
//  }
}

System.err.println("blast has " + count2 + " sequence, " + rr.size() + " unique bases in " + g.size() + " sets");

Set d1 = rr - r;
System.err.println(d1.size() + " sequence bases in blast but not in geneblaster")

Set d2 = r - rr;
System.err.println(d2.size() + " sequence bases in geneblaster but not blast")



//for (k in s.keySet()) {
//  for (x in s.get(k)) {
//    println("<< " + k + "\t" + x);
//  }
//}

