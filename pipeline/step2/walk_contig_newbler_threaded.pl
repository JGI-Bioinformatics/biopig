#!/jgi/tools/bin/perl

use threads;

use File::Path;
use Getopt::Long;

$VMATCH='/home/asczyrba/bin/vmatch';
$VSEQSELECT='/home/asczyrba/bin/vseqselect';
#$VMINDEXDIR='/gpfs/RandD/rumen/DBs/vmindex';
$VMINDEXDIR='/storage/rumen/DBs/vmindex';
@VMINDEXES=qw(s_2 s_3 s_4 s_5 s_6 s_7 s_8);
#@VMINDEXES=qw(s_2);
$MEMTIME='/home/asczyrba/bin/memtime';
$NEWBLER='/home/copeland/local/x86_64/bin/runAssembly';

$max_contig_length = 0;

GetOptions("f=s"     => \$fastafile,
	   "l=s"     => \$overlap_length,
           "start=s" => \$steps_start,
           "stop=s"  => \$steps_stop,
	   "t=s"     => \$NUM_OF_THREADS,
	   "m=s"     => \$max_contig_length,
           );

unless ($fastafile && $overlap_length && $steps_start && $steps_stop && $NUM_OF_THREADS) {
    die "\nusage: $0 -f FASTAFILE -l OVERLAP_LENGTH -start START_STEP -stop STOP_STEP -t NUM_OF_THREADS [-m MAX_CONTIG_LENGTH]

";
}

for ($step=$steps_start; $step<=$steps_stop; $step++) {
    if ($step == 1) {
	$contigfile = $fastafile;
    }
    else {
	$contigfile = "step_".($step-1)."_contigs.fas";
    }

    open(LOG,">>step_".$step.".log") || die "cannot open step_".$step.".log: $!";
    print LOG "\n\n##############################\n";
    print LOG "starting STEP $step...\n";
    print LOG scalar(localtime()),"\n";
    print LOG "##############################\n";
    close LOG;

    ($q_index_ref,$contig_info_ref) = split_newbler_fasta($contigfile,$max_contig_length);
    print STDERR "Read ".@{$q_index_ref}." contigs from $contigfile\n";

    my %total_matches = ();

    foreach $vmindex (@VMINDEXES) {
	my %matches = ();
	my @targets_sorted = ();

	# 
	# find overlaps
	#

	$vm_output_file = "step_".$step."_endoverlaps_".$vmindex."_l".$overlap_length.".vmatch";
	$vm_time_file = "step_".$step."_endoverlaps_".$vmindex."_l".$overlap_length.".vmatch.time";
	
	print STDERR "vmatch input vs $vmindex running...\n";
	$vmatch_call = "$MEMTIME $VMATCH -d -p -selfun /home/asczyrba/vmatch/SELECT/endmatch.so -seedlength 24 -e 2 -l $overlap_length -q $contigfile $VMINDEXDIR/$vmindex > $vm_output_file 2> $vm_time_file";
	print STDERR "$vmatch_call\n";
	system($vmatch_call);
	print STDERR "vmatch done.\n\n";
	
	#
	# get vmatch seqnums for matching sequences
	#
	
	print STDERR "extracting sequence ids for matching pairs...\n";
	open(VM,$vm_output_file) || die "cannot open match file: $!";
	while($vm=<VM>) {
	    next if ($vm =~ /^\#/);
	    $vm =~ s/ +/ /g;
	    $vm =~ s/^ //;
	    @vm = split / /,$vm;
	    $target = $vm[1];
	    $query  = $vm[5];
	    
	    $matches{$target}{$query} = 1;
	    $total_matches{$q_index_ref->[$query]}++;
	}
	close VM;
	
	@targets_sorted = sort {$a <=> $b} keys %matches;

#	print STDERR "targets: ";
#	print STDERR join(" ",@targets_sorted);
#	print STDERR "\n";

	open(SEQNUM,">$vm_output_file.seqnums") || die "cannot open seqnum output file: $!";
	foreach $target (@targets_sorted) {
	    print SEQNUM "$target\n";
	}
	close SEQNUM;
	print STDERR "ids extracted.\n\n";
	
	
	#
	# extract sequences from index and append to contig files
	#
	
	print STDERR "selecting matching sequences from $vmindex...\n";
	$vseqselect_call = "$VSEQSELECT -seqnum $vm_output_file.seqnums $VMINDEXDIR/$vmindex";
	print STDERR "$vseqselect_call\n";
	open(SEQS,"$vseqselect_call |") || die "sequence extraction failed: $!";
	$seqnum = -1;
	while($in = <SEQS>) {
	    chomp $in;
	    if ($in =~ /^>(.*)/) {
		$newdesc = $1;
		if ($seqnum >= 0) {
		    
		    $vm_seqnum = $targets_sorted[$seqnum];
		    foreach $query (keys %{ $matches{$vm_seqnum} }) {
#			print STDERR "$query ";
			$contig = $q_index_ref->[$query];
			$fastaoutfile = $contig."_step_".$step."/".$contig.".fas";
			open(OUT,">>$fastaoutfile") || die "cannot open outfile $fastaoutfile: $!";
#		    print STDERR "Writing to file $fastaoutfile\n";
			print OUT ">$desc\n";
			print OUT "$seq\n";
			close OUT;
		    }
		}
		$seqnum++;
		$desc = $newdesc;
		$seq = '';
	    }
	    else {
		$seq .= $in;
	    }
	}
	close SEQS;
	
	
	$vm_seqnum = $targets_sorted[$seqnum];
	foreach $query (keys %{ $matches{$vm_seqnum} }) {
	    $contig = $q_index_ref->[$query];
	    $fastaoutfile = $contig."_step_".$step."/".$contig.".fas";
	    open(OUT,">>$fastaoutfile") || die "cannot open outfile: $!";
#	print STDERR "Writing to file $fastaoutfile\n";
	    print OUT ">$desc\n";
	    print OUT "$seq\n";
	    close OUT;
	}
	
	print STDERR "vseqselect done.\n\n";
	
    }
    
# 
# call newbler for each contig which has overlapping reads
#

    $match_total=0;

    my @queries = sort keys %total_matches;

    my $job_num=1;
    while(($job_num<=$NUM_OF_THREADS) && ($query=shift @queries)) {
	$match_total++;
	$newbler_file = $query."_step_".$step."/".$query.".fas";
	$outdir =  $query."_step_".$step."/assembly";
	$log_file = $query."_step_".$step.".log";
	
	my ($thr) = threads->create(\&call_newbler,$query,$newbler_file,$outdir,$log_file);
	$job_num++;
    }

    
    open(OUT,">step_".$step."_contigs.fas") || die "cannot open output file: $!";
    while(my $thread_count = threads->list) {
	print STDERR "$thread_count threads running\n";
	foreach my $thread (threads->list(threads::joinable)) {
	    my ($query,$seq,$length,$numreads) = $thread->join();
	    print STDERR "joined thread: $query\n";

	    $contig_info_ref->{$query}->{step_length} = $length;
	    $contig_info_ref->{$query}->{step_numreads} = $numreads;
	    
	    my $extension = $contig_info_ref->{$query}->{step_length}-$contig_info_ref->{$query}->{length};
	    print STDERR "$query extended by $extension bp\n";
	    
	    my $desc = ">$query length=".$contig_info_ref->{$query}->{step_length};
	    $desc .= " numreads=";
	    $desc .= $contig_info_ref->{$query}->{numreads}+$contig_info_ref->{$query}->{step_numreads};
	    $desc .= " (after walking step $step)\n";
	    
	    if ($extension > 0) {
		print OUT $desc;
		print OUT $seq."\n";
	    }

	    # remove newbler output
#	    my $rmoutdir=$query."_step_".$step;
#	    system("rm -rf $rmoutdir");

	    # are thre more queries to assemble?
	    $query=shift @queries;
	    if ($query) {
		print STDERR "calling thread for query: $query\n";
		$match_total++;
		$newbler_file = $query."_step_".$step."/".$query.".fas";
		$outdir =  $query."_step_".$step."/assembly";
		$log_file = $query."_step_".$step.".log";
		my ($thr) = threads->create(\&call_newbler,$query,$newbler_file,$outdir,$log_file);
	    }
	}
	sleep 1;
    }
    close OUT;

    open(LOG,">>step_".$step.".log") || die "cannot open step_".$step.".log: $!";
    print LOG "\n\noverlapping reads found for $match_total/".@{$q_index_ref}." contigs.\n";
    foreach $query (sort keys %{$contig_info_ref}) {
	print LOG $query."\t";
	print LOG $contig_info_ref->{$query}->{length}."\t";
	print LOG $contig_info_ref->{$query}->{step_length}."\t";
	print LOG ($contig_info_ref->{$query}->{step_length}-$contig_info_ref->{$query}->{length});
	print LOG "\t";
	print LOG $contig_info_ref->{$query}->{step_numreads}."\n";
    }


    print LOG "\n\n##############################\n";
    print LOG "STEP $step done.\n";
    print LOG scalar(localtime()),"\n";
    print LOG "##############################\n";
    close LOG;
    
}



##############################################################################

sub split_newbler_fasta {
    my $fastafile = shift;
    my $max_contig_length=shift;
    my $seqnum=-1;

    my $contig=undef;
    @q_index = ();
    %contig_info = ();

    open INFILE,$fastafile or die "Can't open $fastafile: $!";
    while(<INFILE>) {
	chomp;
	s///g;
	if (/^>(\S+)\s+length=(\d+)\s+numreads=(\d+)/) {
	    $newcontig = $1;
	    $newlength = $2;
	    $newnumreads = $3;
	    if ((defined $contig) && ($length < $max_contig_length)) {
		$seqnum++;
		$q_index[$seqnum]=$contig;
		$contig_info{$contig}{length}=$length;
		$contig_info{$contig}{numreads}=$numreads;
		$outdir=$contig."_step_$step";
#		print STDERR "mkdir $outdir\n";
		mkdir $outdir || die "cannot mkdir \"$outdir\": $!";
		open(OUT,">$outdir/$contig.fas") || die "cannot open outfile \"$outdir/$contig.fas\": $!";
#		print STDERR "writing $outdir/$contig.fas\n";
		my $desc = "$contig length=$length numreads=$numreads";
		if (length($seq) > 1500) {
		    my $shredded_seq = shred_fasta($desc,$seq,1000,200);
		    print OUT $shredded_seq;
		}
		else {
		    print OUT ">$desc\n";
		    print OUT $seq."\n";
		}
		close OUT;
	    }
	    if ($length >= $max_contig_length) {
		print STDERR "length of $contig: $length >= max_contig_length ($max_contig_length), skipping...)\n";
	    }
	    
	    $contig = $newcontig;
	    $length = $newlength;
	    $numreads = $newnumreads;
	    $seq = '';
	}
	else {
	    $seq .= $_;
	}
    }

    if ($length < $max_contig_length) {
	$seqnum++;
	$q_index[$seqnum]=$contig;
	$contig_info{$contig}{length}=$length;
	$contig_info{$contig}{numreads}=$numreads;
	$outdir=$contig."_step_$step";
	mkdir $outdir;
	open(OUT,">$outdir/$contig.fas") || die "cannot open outfile: $!";
	my $desc = "$contig length=$length numreads=$numreads";
	if (length($seq) > 1500) {
	    my $shredded_seq = shred_fasta($desc,$seq,1000,200);
	    print OUT $shredded_seq;
	}
	else {
	    print OUT ">$desc\n";
	    print OUT $seq."\n";
	}
	close OUT;
    }
    else {
	print STDERR "length of $contig ($length) >= max_contig_length ($max_contig_length), skipping...)\n";
    }

    return (\@q_index,\%contig_info);
}


sub shred_fasta {
    my $desc = shift;
    my $seq = shift;
    my $length = shift;
    my $overlap = shift;
    my $shredded_seq;

    die "ERROR: overlap > length" if ($overlap > $length);

    for($i=0; $i<=(length($seq)-$overlap); $i+=($length-$overlap)) {
	my $stop=$i+$length;
	$stop = length($seq) if ($stop>length($seq));
	my $substr = substr($seq,$i,$length);
	if ($substr) {
	    $shredded_seq .= ">$desc ($i..$stop)\n";
	    $shredded_seq .=  "$substr\n";
	}
    }
    return ($shredded_seq);
}




sub call_newbler {
    my ($query,$newbler_file,$outdir,$log_file) = @_;

    print STDERR "assembling $query...\n";
#    $assembly_call = "$NEWBLER -p $newbler_file  -o $outdir -rip -consed > $log_file";
#    $assembly_call = "$NEWBLER -p $newbler_file  -o $outdir -rip > $log_file";
    $assembly_call = "$NEWBLER -p $newbler_file  -o $outdir -rip > /dev/null";
    print STDERR "$assembly_call\n";
    system($assembly_call);

    my $length =0;
    my $seq = '';
    my $numreads=0;

    open(ASSEM,"$outdir/454LargeContigs.fna") || die "cannot find $outdir/454LargeContigs.fna: $!";
    while(<ASSEM>) {
	if (/length=(\d+)\s+numreads=(\d+)/) {
	    $length = $1;
	    $numreads = $2;
	}
	else {
	    chomp;
	    $seq .= $_;
	}
    }
    close ASSEM;
	
    return($query,$seq,$length,$numreads);
}
