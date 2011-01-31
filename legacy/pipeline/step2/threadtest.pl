#!/jgi/tools/bin/perl -w

use strict;
use threads;
use Log::Log4perl qw(get_logger :levels);

# read logging configuration from file
Log::Log4perl->init("log.conf");
my $log = get_logger();
$log->level($INFO);

my %CONFIG;

$CONFIG{NUM_OF_THREADS} = 20;

my @queries = (1..10000);

my $job_num=1;
while(($job_num<=$CONFIG{NUM_OF_THREADS}) && (my $query=shift @queries)) {
    my ($thr) = threads->create(\&call_cap3,$query);
    $job_num++;
}

while(my $thread_count = threads->list) {
    print STDERR ("$thread_count threads running\n");
    $log->info ("$thread_count threads running");
    foreach my $thread (threads->list(threads::joinable)) {
	my ($result) = $thread->join();
	$log->info("joined thread: $result");

	# are thre more queries to assemble?
	my $query=shift @queries;
	if ($query) {
	    $log->info("calling thread for query: $query");
	    my ($thr) = threads->create(\&call_cap3,$query);
	}
    }
#    sleep 1;
}

sub call_cap3 {
    my ($query) = @_;

#    my $log = get_logger();

    my $j=0;
    $log->info("sleeping 5 secs");
    print STDERR "thread $query, sleeping for 5 secs...\n";
    #for (my $i=0; $i<=500000; $i++) {$j++;}
    system("cap3 &> /dev/null");

    return($query);
}

