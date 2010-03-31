#!/usr/bin/perl

use threads;
use strict;
use Getopt::Long;

my $start;
my $end;
my $last;

local $| = 1;

$start = time();
print "started script: timestame = $start\n";

print "start setup ... ";

my $EC;
my $workdir;

GetOptions("ec=s"     => \$EC,
           "d=s"      => \$workdir,
           );

unless ($EC && $workdir) {
    die "\nusage: $0 -d  => WORKDIR -e EC_NUMBER

";
}


# do a family based analysis to test assembly and discover new genes
our $cazy = "/house/homedirs/k/kbhatia/pipeline/EC3.2.1.4.faa";
#our $cazy = "/house/homedirs/k/kbhatia/pipeline/ECall_fl_final_primer_20090814_nr_BLASTX_based_ORFs.faa";
our $seqDir = "/house/homedirs/k/kbhatia/pipeline/Illumina";
our @lanes = (2..8);

# for assembly
my $largeContig = 500;
my $minIdentity = 98;
my $minOverlap = 40;


#my $blast_db = "/house/homedirs/k/kbhatia/pipeline/rawdata.fa";
my $blast_db = "/storage/rumen/DBs/BLAST/all_reads";
#my $blast_cmd = "/home/asczyrba/src/blast-2.2.20/bin/blastall -m 8 -p tblastn -b 1000000 -a 10 -o $workdir/EC$EC.blastout -d $blast_db -i EC$EC.faa";
my $blast_cmd = "/home/asczyrba/src/blast-2.2.20/bin/blastall -m 8 -p tblastn -b 1000000 -a 10 -o $workdir/cazy.blastout -d $blast_db -i $cazy";

$end = time();
print " done time = ", $end - $start, "\n";
$last = $end;
print "start filter EC ...";


#unless (-e "$workdir/EC$EC.faa") {
#    grep_EC_in_cazy($cazy,$EC,"EC$EC.faa");
#}

$end = time();
print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
$last = $end;

print "start blast ";


unless(-e "$workdir/cazy.blastout") {
    print STDERR "$blast_cmd\n";
    system($blast_cmd);
}

$end = time();
print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
$last = $end;

print "seeding blat ... ";

unless(-e "$workdir/cazy.blatout") {
    seedBLAT("$workdir/cazy.blastout", "$workdir/cazy.blatout");
}

$end = time();
print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
$last = $end;


print "assembly ... ";

unless(-e "$workdir/cazy.PE_Assembly") {# newbler assembly
	system("/home/copeland/local/x86_64/newbler/2.3-PreRelease-10-20-2009-gcc-4.1.2-threads/runAssembly -m -consed -l $largeContig -mi $minIdentity -ml $minOverlap -o $workdir/cazy.PE_Assembly -p $workdir/cazy.blatout > /dev/null 2>&1");
}

$end = time();
print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
$last = $end;

sub seedBLAT
{ # input a blast output, output its pairs, and homologous pairs (via blat)
	my ($input, $output) = @_;

	my %prey;
	# parse the original blast output
	my %seqs;
	
	print "\tcreating inputs ...";
	
	open(IN,$input) || die "cannot open input BLAST file for parsing: $!";
	while(my $in = <IN>) {
	    my @data = split /\t/, $in;
	    my $read = $data[1];
	    chop($read);
	    $read .= "1";
	    $seqs{$read}++;
	}
	close IN;

	# get its mate pair
	getMate(\%seqs, "$input-0-unique-reads.fa");

	$end = time();
	print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
	$last = $end;
	
	# blat search
	print "\trunning blat ...";
	foreach my $lane (@lanes) {
	    my $thr;
	    ($thr) = threads->create(\&runBLAT,"$seqDir/s_$lane\_1.fa","$input-0-unique-reads.fa","$input-seed.blat.$lane.1");
	    ($thr) = threads->create(\&runBLAT,"$seqDir/s_$lane\_2.fa","$input-0-unique-reads.fa","$input-seed.blat.$lane.2");
	}

	while(threads->list()) {
	    foreach my $thread (threads->list(threads::joinable)) {
		$thread->join();
	    }
	}
	$end = time();
	print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
	$last = $end;

	print "\tpost processing ...";
	
	system("cat $input-seed.blat.* > $input-seed.blat");
	system("rm $input-seed.blat.*");
	# get mate pair for newbler assembly
	my @reads = `cut -f2 $input-seed.blat`;
	foreach my $read (@reads) {
		chomp($read);
		chop($read);
		$read.="1";
		$seqs{$read}++;
	}
#	system("rm $input-seed.blat");

	my $i=0;
	foreach my $t (keys %seqs) {
	    $i++;
	}
	print "$i reads\n";

	get454Mate(\%seqs, $output);
	
	$end = time();
	print " done time = ",  $end - $start, "( ", $end - $last, " )\n";
	$last = $end;
}

sub runBLAT {
    my $blat_db = shift;
    my $blat_query = shift;
    my $blat_output = shift;
    my $blat = "blat -out=blast8";

    print STDERR "THREAD: $blat $blat_db $blat_query $blat_output\n";
    system("$blat $blat_db $blat_query $blat_output");
}


sub getMate
{
    my ($seqs,$outfile)=@_;
    open(OUT, ">$outfile");
    foreach my $lane (@lanes) {
	open(IN1, "$seqDir/s_$lane\_1.fa") or die $!;
	open(IN2, "$seqDir/s_$lane\_2.fa") or die $!;
	while(my $name1=<IN1> and my $seq1=<IN1> and my $name2=<IN2> and my $seq2=<IN2>) {
	    chomp($name1); chomp($name2);
	    my $read = '';
	    if ($name1 =~ />(.*)/) {
		$read = $1;
	    }
	    print OUT "$name1\n$seq1", "$name2\n$seq2" if($seqs->{$read}>0);
	}
	close(IN1);
	close(IN2);
    }
    close(OUT);
}

sub get454Mate
{
    my ($seqs,$outfile)=@_;
    open(OUT, ">$outfile");
    foreach my $lane (@lanes) {
	print STDERR "get454Mate: reading $seqDir/s_$lane\_1.fa and $seqDir/s_$lane\_2.fa...\n";
	open(IN1, "$seqDir/s_$lane\_1.fa") or die $!;
	open(IN2, "$seqDir/s_$lane\_2.fa") or die $!;
	while(my $name1=<IN1> and my $seq1=<IN1> and my $name2=<IN2> and my $seq2=<IN2>) {
	    chomp($name1); chomp($name2);
	    my $read = '';
	    if ($name1 =~ />(.*)/) {
		$read = $1;
	    }
	    if($seqs->{$read}>0) {
		substr($name1, -3, 3, '_left');
		substr($name2, -3, 3, '_right');
		print OUT "$name1\n$seq1", "$name2\n$seq2";
	    }
	}
	close(IN1);
	close(IN2);
    }
    close(OUT);
}



sub grep_EC_in_cazy {
    my $fastafile = shift;
    my $ec_number = shift;
    my $outfile = shift;

    my $entry;
    my $acc;
    my $ecs;
    my %seen;

    open(OUT, ">$outfile") || die "cannot open outfile: $!";

    open INFILE,$fastafile or die "Can't open: $fastafile";
    my $description=<INFILE>;
    while (my $in = <INFILE>) {
	unless ($in =~ /^>\S+/) {
	    $entry .= $in;
	}
	else {
	    if ($description =~ /^>(\S+) .*EC=(\S+)/) {
		$acc=$1;
		$ecs=$2;
		$seen{$acc}++;
	    }
	    foreach my $ec (split ",", $ecs) {
		if (($seen{$acc}==1) && ($ec eq $ec_number)) {
		    print OUT $description.$entry;
		}
	    }

	    $description=$in;
	    $entry="";
	}
    }

    close INFILE;

    # check last entry

    if ($description =~ /^>(\S+) .*EC=(\S+)/) {
	$acc=$1;
	$ecs=$2;
	$seen{$acc}++;
    }
    foreach my $ec (split ",", $ecs) {
	if (($seen{$acc}==1) && ($ec eq $ec_number)) {
	    print OUT $description.$entry;
	}
    }
    
    close OUT;
}
