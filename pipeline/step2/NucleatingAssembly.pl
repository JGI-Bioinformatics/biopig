#!/jgi/tools/bin/perl -w

use strict;
use threads;
use Getopt::Long;
use Config::General;
use File::Path;
use Digest::MD5 qw(md5_hex);
use Log::Log4perl qw(get_logger :levels);

# read logging configuration from file
Log::Log4perl->init("log.conf");
my $log = get_logger();
$log->level($INFO);

my $configfile;
my $opt_debug;

GetOptions("c=s"     => \$configfile,
	   "debug"   => \$opt_debug,
           );

unless ($configfile) {
    die "\nusage: $0 -c configfile [-debug]

";
}

if ($opt_debug) {
    $log->level($DEBUG);
}


# read config file
my %CONFIG = new Config::General($configfile)->getall;

foreach my $option (keys %CONFIG) {
    $log->info($option,"=",$CONFIG{$option});
}


############################################################
# for each reads file, check if vmindex exists
# if not, create it
############################################################

foreach my $readsfile (sort split(/,/,$CONFIG{READSFILES})) {
    my $fq_readsfile = $CONFIG{READSDIR}."/$readsfile";
    my $fq_indexfile = $CONFIG{INDEXDIR}."/$readsfile";
    $log->info("checking for reads file $fq_readsfile");
    unless (-e $fq_readsfile) {
	$log->logdie("reads file $fq_readsfile does not exist");
    }
    $log->info("checking for index file $fq_indexfile.prj");
    unless (-e "$fq_indexfile.prj") {
	$log->info("$fq_indexfile.prj not found, running mkvtree");
	unless (-e $CONFIG{INDEXDIR}) {
	    mkpath($CONFIG{INDEXDIR});
	}
	system($CONFIG{VMATCH_PATH}."/mkvtree -dna -allout -pl -db $fq_readsfile -indexname $fq_indexfile");
    }
    else {
	$log->info("$fq_indexfile found!");
    }
}


unless (-e $CONFIG{WORKDIR}) {
    mkpath($CONFIG{WORKDIR});
}

for (my $step=$CONFIG{START_STEP}; $step<$CONFIG{START_STEP}+$CONFIG{NUM_OF_STEPS}; $step++) {
    my $contigfile = $CONFIG{WORKDIR}."/"."step_".($step-1)."_contigs.fas";
    $contigfile = $CONFIG{CONTIGFILE} if ($step==1);

    $log->info("############################################################");
    $log->info("starting STEP $step...");
    $log->info(scalar(localtime()));
    $log->info("############################################################");

    (my $q_index_ref,my $contig_info_ref) = split_input_fasta($contigfile,$step);

    $log->info("Read ".@{$q_index_ref}." contigs from $contigfile");

    my $total_matches_ref = find_overlaps($contigfile,$step,$q_index_ref);

    newbler_assembly($total_matches_ref,$contig_info_ref,$q_index_ref,$step);
}


sub find_overlaps {
    my $contigfile = shift;
    my $step = shift;
    my $q_index_ref = shift;

    my %total_matches = ();
    my $log = get_logger();

    foreach my $vmindex (split /,/,$CONFIG{READSFILES}) {
	my %matches = ();
	my @targets_sorted = ();

	# 
	# find overlaps
	#

	my $vm_output_file = ($CONFIG{WORKDIR}."/step_".$step."_endoverlaps_".$vmindex."_l".
			      $CONFIG{MIN_OVERLAP}."e".$CONFIG{MAX_ERRORS_IN_OVERLAP}.".vmatch");
	my $vm_time_file = $vm_output_file.".time";
	
	$log->info("vmatch input vs $vmindex running...");
	my $vmatch_call = ($CONFIG{TIME}." ".$CONFIG{VMATCH_PATH}."/vmatch -d -p ".
			   "-selfun /home/asczyrba/vmatch/SELECT/endmatch.so ".
			   "-seedlength 24 -e ".$CONFIG{MAX_ERRORS_IN_OVERLAP}.
			   " -l ".$CONFIG{MIN_OVERLAP}." -q $contigfile ".
			   $CONFIG{INDEXDIR}."/$vmindex > $vm_output_file 2> $vm_time_file");
	$log->info("$vmatch_call");
	system($vmatch_call);
	$log->info("vmatch done.");
	
	#
	# get vmatch seqnums for matching sequences
	#
	
	$log->info("extracting sequence ids for matching pairs...");
	open(VM,$vm_output_file) || $log->logdie("cannot open match file: $!");
	while(my $vm=<VM>) {
	    next if ($vm =~ /^\#/);
	    $vm =~ s/ +/ /g;
	    $vm =~ s/^ //;
	    my @vm = split / /,$vm;
	    my $target = $vm[1];
	    my $query  = $vm[5];
	 
	    # check if query was removed because of max_contig_length cutoff
	    if (defined $q_index_ref->[$query]) {
		#print STDERR "$query\n";
		#print STDERR "q_index_ref($query):".$q_index_ref->[$query]."\n";
		$matches{$target}{$query} = 1;
		$total_matches{$q_index_ref->[$query]}++;
	    }
	}
	close VM;
	
	@targets_sorted = sort {$a <=> $b} keys %matches;

#	print STDERR "targets: ";
#	print STDERR join(" ",@targets_sorted);
#	print STDERR "\n";

	open(SEQNUM,">$vm_output_file.seqnums") || $log->logdie("cannot open seqnum output file: $!");
	foreach my $target (@targets_sorted) {
	    print SEQNUM "$target\n";
	}
	close SEQNUM;
	$log->info("ids extracted.");
	
	
	#
	# extract sequences from index and append to contig files
	#
	
	$log->info("selecting matching sequences from $vmindex...");
	my $vseqselect_call = $CONFIG{VMATCH_PATH}."/vseqselect -seqnum $vm_output_file.seqnums ".$CONFIG{INDEXDIR}."/$vmindex";
	$log->info("$vseqselect_call");
	open(SEQS,"$vseqselect_call |") || $log->logdie("sequence extraction failed: $!");
	my $seqnum = -1;
	my $desc='';
	my $seq='';
	while(my $in = <SEQS>) {
	    chomp $in;
	    if ($in =~ /^>(.*)/) {
		my $newdesc = $1;
		if ($seqnum >= 0) {
		    
		    my $vm_seqnum = $targets_sorted[$seqnum];
		    foreach my $query (keys %{ $matches{$vm_seqnum} }) {
			my $contig = $q_index_ref->[$query];
			my $fastaoutfile = $CONFIG{WORKDIR}."/".substr(md5_hex($contig),0,2)."/".$contig."_step_".$step."/".$contig.".fas";
			$log->debug("adding $desc to contig $contig ($fastaoutfile)");
			open(OUT,">>$fastaoutfile") || $log->logdie("cannot open outfile $fastaoutfile: $!");
			print OUT ">$desc\n";
			$seq =~ s/(.{60})/$1\n/g;
			print OUT "$seq\n";
			close OUT;
		    }
		}
		$seqnum++;
		$desc = $newdesc;
		$seq = '';
	    }
	    else {
		chomp $in;
		$seq .= $in;
	    }
	}
	close SEQS;
	
	
	my $vm_seqnum = $targets_sorted[$seqnum];
	foreach my $query (keys %{ $matches{$vm_seqnum} }) {
	    my $contig = $q_index_ref->[$query];
	    my $fastaoutfile = $CONFIG{WORKDIR}."/".substr(md5_hex($contig),0,2)."/".$contig."_step_".$step."/".$contig.".fas";
	    $log->debug("adding $desc to contig $contig ($fastaoutfile)");
	    open(OUT,">>$fastaoutfile") || $log->logdie("cannot open outfile: $!");
	    print OUT ">$desc\n";
	    print OUT "$seq\n";
	    close OUT;
	}
	
	$log->info("vseqselect done.");
	
    }

    return(\%total_matches);
}

sub newbler_assembly {
    # 
    # call newbler for each contig which has overlapping reads
    #

    my $total_matches_ref = shift;
    my $contig_info_ref = shift;
    my $q_index_ref = shift;
    my $step = shift;

    my $match_total=0;

    my @queries = sort keys %{ $total_matches_ref};

    my $job_num=1;
    while(($job_num<=$CONFIG{NUM_OF_THREADS}) && (my $query=shift @queries)) {
	$match_total++;
	my $newbler_file = $CONFIG{WORKDIR}."/".substr(md5_hex($query),0,2)."/".$query."_step_".$step."/".$query.".fas";
	my $outdir =  $CONFIG{WORKDIR}."/".substr(md5_hex($query),0,2)."/".$query."_step_".$step."/assembly";
	my $log_file = $CONFIG{WORKDIR}."/".substr(md5_hex($query),0,2)."/".$query."_step_".$step.".log";
	
	#my ($thr) = threads->create(\&call_newbler,$query,$newbler_file,$outdir,$log_file);
	my ($thr) = threads->create(\&call_cap3,$query,$newbler_file,$outdir,$log_file);
	$job_num++;
    }

    
    open(OUT,">".$CONFIG{WORKDIR}."/step_".$step."_contigs.fas") || $log->logdie("cannot open output file: $!");
    while(my $thread_count = threads->list) {
#	print STDERR ("$thread_count threads running\n");
	$log->info ("$thread_count threads running");
	foreach my $thread (threads->list(threads::joinable)) {
	    my ($query,$seq,$length) = $thread->join();
	    $log->info("joined thread: $query");

	    #$log->debug("RETURN:$query,$seq,$length");
	    $contig_info_ref->{$query}->{step_length} = $length;
	    
	    my $extension = $contig_info_ref->{$query}->{step_length}-$contig_info_ref->{$query}->{length};
	    $log->info("$query extended by $extension bp");
	    
	    my $desc = ">$query length=".$contig_info_ref->{$query}->{step_length};
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
		$log->info("calling thread for query: $query");
		$match_total++;
		my $newbler_file = $CONFIG{WORKDIR}."/".substr(md5_hex($query),0,2)."/".$query."_step_".$step."/".$query.".fas";
		my $outdir =  $CONFIG{WORKDIR}."/".substr(md5_hex($query),0,2)."/".$query."_step_".$step."/assembly";
		my $log_file = $CONFIG{WORKDIR}."/".substr(md5_hex($query),0,2)."/".$query."_step_".$step.".log";
		#my ($thr) = threads->create(\&call_newbler,$query,$newbler_file,$outdir,$log_file);
		my ($thr) = threads->create(\&call_cap3,$query,$newbler_file,$outdir,$log_file);
	    }
	}
	#sleep 1;
    }
    close OUT;

    $log->info("overlapping reads found for $match_total/".@{$q_index_ref}." contigs.");
    foreach my $query (sort keys %{$contig_info_ref}) {
	if (exists $contig_info_ref->{$query}->{step_length}) {
	    $log->debug("query:".$query.":");
	    $log->debug("contig_info_ref->{$query}->{length}".
			$contig_info_ref->{$query}->{length});
	    $log->debug("contig_info_ref->{$query}->{step_length}:".
			$contig_info_ref->{$query}->{step_length});
	    $log->debug("diff:".($contig_info_ref->{$query}->{step_length}
				 -$contig_info_ref->{$query}->{length}));
	}
    }


    $log->info("##############################");
    $log->info("STEP $step done.");
    $log->info(scalar(localtime()));
    $log->info("##############################");

}







##############################################################################

sub split_input_fasta {
    my $fastafile = shift;
    my $step = shift;
    my $seqnum=-1;

    my @q_index = ();
    my %contig_info = ();
    my $contig = undef;
    my $length = undef;
    my $seq = '';

    my $log = get_logger();

    open INFILE,$fastafile or $log->logdie("Can't open contig file \"$fastafile\": $!");
    while(<INFILE>) {
	chomp;
	s///g;
#	if (/^>(\S+)\s+length=(\d+)\s+numreads=(\d+)/) {
	if (/^>(\S+)\s*/) {
	    my $newcontig = $1;
	    $length = length($seq);
	    if ((defined $contig) && ($length < $CONFIG{MAX_CONTIG_LENGTH})) {
		$seqnum++;
		$q_index[$seqnum]=$contig;
		$contig_info{$contig}{length}=$length;
		$log->debug("contig $contig has seqnum $seqnum and length $length");
		my $outdir=$CONFIG{WORKDIR}."/".substr(md5_hex($contig),0,2)."/".$contig."_step_$step";
		$log->debug("mkdir $outdir");
		mkpath $outdir || $log->logdie("cannot mkdir \"$outdir\": $!");
		open(OUT,">$outdir/$contig.fas") || $log->logdie("cannot open outfile \"$outdir/$contig.fas\": $!");
		$log->debug("writing $outdir/$contig.fas");
		my $desc = "$contig length=$length";
		if ($length > 1500) {
		    my $shredded_seq = shred_fasta($desc,$seq,1000,200);
		    print OUT $shredded_seq;
		}
		else {
		    print OUT ">$desc\n";
		    print OUT $seq."\n";
		}
		close OUT;
	    }
	    if ($length >= $CONFIG{MAX_CONTIG_LENGTH}) {
		$log->info("length of $contig: $length >= max_contig_length (".$CONFIG{MAX_CONTIG_LENGTH}."), skipping...)");
	    }
	    
	    $contig = $newcontig;
	    $seq = '';
	}
	else {
	    chomp;
	    $seq .= $_;
	}
    }

    if ($length < $CONFIG{MAX_CONTIG_LENGTH}) {
	$seqnum++;
	$q_index[$seqnum]=$contig;
	$log->debug("contig $contig has seqnum $seqnum");
	$contig_info{$contig}{length}=$length;
	my $outdir=$CONFIG{WORKDIR}."/".substr(md5_hex($contig),0,2)."/".$contig."_step_$step";
	$log->debug("mkdir $outdir");
	mkpath $outdir || $log->logdie("cannot mkdir \"$outdir\": $!");
	$log->debug("writing $outdir/$contig.fas");
	open(OUT,">$outdir/$contig.fas") || $log->logdie("cannot open outfile \"$outdir/$contig.fas\": $!");
	my $desc = "$contig length=$length";
	if ($length > 1500) {
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
	$log->info("length of $contig ($length) >= max_contig_length (".$CONFIG{MAX_CONTIG_LENGTH}."), skipping...)");
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

    for(my $i=0; $i<=(length($seq)-$overlap); $i+=($length-$overlap)) {
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

    my $log = get_logger();

    $log->info("assembling $query...");
#    my $assembly_call = $CONFIG{NEWBLER_BIN}." -p $newbler_file  -o $outdir -rip -consed > $log_file";
#    my $assembly_call = $CONFIG{NEWBLER_BIN}." -p $newbler_file  -o $outdir -rip > /dev/null";
    my $assembly_call = $CONFIG{NEWBLER_BIN}." -p $newbler_file  -o $outdir -rip > $log_file";
    $log->info("$assembly_call");
    system($assembly_call);

    # find longest contig
    my $seq = '';
    my $longest_seq = '';

    open(ASSEM,"$outdir/454LargeContigs.fna") || $log->logdie("cannot find $outdir/454LargeContigs.fna: $!");
    while(<ASSEM>) {
	if (/^>/) {
	    if (length($seq) > length($longest_seq)) {
		$longest_seq = $seq;
	    }
	    $seq = '';
	}
	else {
	    chomp;
	    $seq .= $_;
	}
    }
    close ASSEM;

    if (length($seq) > length($longest_seq)) {
	$longest_seq = $seq;
    }

    return($query,$longest_seq,length($longest_seq));
}

sub call_cap3 {
    my ($query,$inputfile,$outdir,$log_file) = @_;

    my $log = get_logger();

    $log->info("cap3 assembling $query...");
    my $assembly_call = $CONFIG{CAP3_BIN}." $inputfile > $log_file";
    $log->info("$assembly_call");
    system($assembly_call);

    # find longest contig
    my $seq = '';
    my $longest_seq = '';

    open(ASSEM,"$inputfile.cap.contigs") || $log->logdie("cannot find $inputfile.cap.contigs: $!");
    while(<ASSEM>) {
	if (/^>/) {
	    if (length($seq) > length($longest_seq)) {
		$longest_seq = $seq;
	    }
	    $seq = '';
	}
	else {
	    chomp;
	    $seq .= $_;
	}
    }
    close ASSEM;

    if (length($seq) > length($longest_seq)) {
	$longest_seq = $seq;
    }

    return($query,$longest_seq,length($longest_seq));
}

