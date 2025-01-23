#!/usr/bin/perl

use DBI;
use Roman;

my $path = $0;
$path =~ s|[^/]+$||;
$path = "./" . $path if ($path !~ m|^/|);
require("$path/config.pl");

my %field_map = (
    'Accession' => null,
    'AU' => null,
    'DAY' => 'day',
    'DOI' => 'doi',
    'FPAGE' => 'first_page',
    'IL' => 'illustration',
    'IMP' => null,
    'ISSN' => 'issn',
    'ISSUE' => 'issue',
    'JOURNAL' => 'journal',
    'LPAGE' => 'last_page',
    'MED' => 'media',
    'MONTH' => 'month',
    'PAGES' => 'page_range',
    'PEER_REVIEW' => 'peer_review',
    'REV' => 'reviewer',
    'SEASON' => 'season',
    'SUB' => null,
    'TI' => 'title',
    'TYPE' => 'type',
    'URL' => 'url',
    'VOL' => 'volume',
    'YEAR' => 'year'
);

my $dbconn = dbInit($db_host, $db_name, $db_user, $db_password);

if ($#ARGV != 3) {
    print "Usage: $0 <input_file> <new_records_output_file> <duplicate_records_output_file> <invalid_records_output_file>\n";
    exit 1;
}

open ($input, $ARGV[0]) or die "Can't read $ARGV[0]: $!\n";

if (-f $ARGV[1]) {
    print "$ARGV[1] already exists and I won't overwrite it. Please remove or rename it.\n";
    exit 1;
}

if (-f $ARGV[2]) {
    print "$ARGV[2] already exists and I won't overwrite it. Please remove or rename it.\n";
    exit 1;
}

if (-f $ARGV[3]) {
    print "$ARGV[3] already exists and I won't overwrite it. Please remove or rename it.\n";
    exit 1;
}

print "Reading records from: $ARGV[0]\n";
print "Writing new records to: $ARGV[1]\n";
print "Writing duplicate records to: $ARGV[2]\n";
print "Writing invalid records to: $ARGV[3]\n";
print "\n";

my $issn_data;
my $journals_not_found;

print "Scanning for journals\n\n";

while (my $line = <$input>) {
    chomp $line;
    if ($line =~ /^JOURNAL (.*)$/) {
	my $journal = $1;
	my $data = findISSN($dbconn, $journal);
	if (defined($data)) {
	    $issn_data->{$journal} = $data;
	}
	else {
	    $journals_not_found->{$journal}++;
	}
    }
}

close $input;

if (keys(%$journals_not_found) > 0) {
    if (keys(%$journals_not_found) == 1) {
        print "The following journal is missing an entry in the ISSN ttable:\n";
    }
    else {
        print "The following " . keys(%$journals_not_found) . " journals are missing entries in the ISSN ttable:\n";
    }

    foreach my $j (keys(%$journals_not_found)) {
        my $times = $journals_not_found->{$j};
        print "$j ($times record";
        print "s" if ($times > 1);
        print ")\n";
    }

    $dbconn->disconnect() if ($dbconn);
    close $input;
    exit 1;
}

if (keys(%$issn_data) == 1) {
    print "Found 1 journal in the input.\n\n";
} else {
    print "Found " . keys(%$issn_data) . " journals in the input.\n\n";
}

open ($input, $ARGV[0]) or die "Can't read $ARGV[0]: $!\n";
open ($good_output, ">".$ARGV[1]) or die "Can't write to $ARGV[1]: $!\n";
open ($bad_output, ">".$ARGV[2]) or die "Can't write to $ARGV[2]: $!\n";
open ($ugly_output, ">".$ARGV[3]) or die "Can't write to $ARGV[3]: $!\n";

my $count = 0;
my $good_count = 0;
my $bad_count = 0;
my $ugly_count = 0;
$| = 1;

while (my $record = getNextRecord($input)) {
    print "\rRead records: $count" if ($count%10 == 0);
    my $data = parseRecord($record, \%field_map);

    if (!isValidRecord($data)) {
        print $ugly_output "\$\n" if ($ugly_count == 0);
        print $ugly_output $record . "\$\n";
	$ugly_count++;
    }
    elsif (findRecord($dbconn, $data, \%field_map)) {
        print $bad_output "\$\n" if ($bad_count == 0);
        print $bad_output $record . "\$\n";
	$bad_count++;
    }
    else {
        print $good_output "\$\n" if ($good_count == 0);
        print $good_output $record . "\$\n";
	$good_count++;
    }
    
    undef $data;

    $count++ if ($record ne "");
}

$dbconn->disconnect() if ($dbconn);
close $input;
close $good_output;
close $bad_output;
close $ugly_output;
unlink $ARGV[1] if ($good_count == 0);
unlink $ARGV[2] if ($bad_count == 0);
unlink $ARGV[3] if ($ugly_count == 0);

print "\r";
print "Read records: $count\n";
print "Found inbalid records: $ugly_count\n";
print "Found duplicate records: $bad_count\n";
print "Found new records: $good_count\n";

exit 0;

sub getNextRecord {
    my ($fh) = @_;
    my $record = "";

    while ($line = <$fh>) {
	chomp $line;

	# End of record
	if ($line eq "\$") {
	    # Or is this the beginning of the file?
	    next if ($record eq "");

	    # Nope, it really is the end of the record
	    return $record;
	}

	$record .= $line . "\n";
    }

    # If you're still here, we've hit the end of the file, return null
    return;
}

sub parseRecord {
    my ($record, $field_map) = @_;
    my $last_key = "";
    my %field;

    foreach my $line (split("\n", $record)) {
	$line =~ s/ +$//;
	my ($key, $val) = split (" ", $line, 2);

	if ($field_map->{$key}) {
	    next if ($val eq "");

	    if ($key eq "Accession") {
		$val =~ s/^no\. //;
	    }
	    elsif ($key eq "TYPE") {
		$val =~ s/^JOURNAL //;
	    }
	    elsif ($key eq "MONTH" && !isMonth($val)) {
		$key = "SEASON";
	    }
	    elsif ($key eq "PAGES") {
		($field{FPAGE},$field{LPAGE}) = getFirstAndLastPages($val);
	    }

	    $last_key = $key;

	    if ($key eq "AU" || $key eq "SUB") {
		push (@{$field{$key}}, $val);
	    }
	    else {
		$field{$key} = $val;
	    }
	}
	elsif ($last_key ne "") {
	    if ($last_key eq "AU" || $last_key eq "SUB") {
		$field{$last_key}->[$#{$field{$last_key}}] .= " " . $line;
	    }
	    else {
		$field{$last_key} .= " " . $line;
	    }
	}
    }

    return \%field;
}

sub isValidRecord {
    my ($data) = @_;

    return (($data->{TYPE} eq "ARTICLE" || $data->{TYPE} eq "REVIEW")
	&& $data->{TI} && $data->{YEAR} && $data->{JOURNAL});
}

sub isMonth {
    my ($field) = @_;

    return ($field =~ /^jan/i ||
	$field =~ /^feb/i ||
	$field =~ /^mar/i ||
	$field =~ /^apr/i ||
	$field =~ /^may/i ||
	$field =~ /^jun/i ||
	$field =~ /^jul/i ||
	$field =~ /^aug/i ||
	$field =~ /^sep/i ||
	$field =~ /^oct/i ||
	$field =~ /^nov/i ||
	$field =~ /^dec/i);
}

sub getFirstAndLastPages {
    my ($range) = @_;
    my ($first, $last);

    if ($range =~ /^([^,+-]+)/) {
	$first = $1;

	if ($range =~ /[, -]+([^,+-]+)\+?$/) {
	    $last = $1;
	}
	else {
	    $last = $first;
	}
    }
    else {
	$first = $range;
	$last = $range;
    }
    
    return ($first, $last);
}

sub dbInit {
    my ($host, $db, $user, $pw) = @_;

    my $dsn = "DBI:mysql:database=$db;host=$host";
    my $dbh = DBI->connect($dsn, $user, $pw,
	{ RaiseError => 1, mysql_auto_reconnect => 1});

    return $dbh;
}

sub findRecord {
    my ($dbh, $data, $field_map) = @_;
    my $accession;

    my @where;

    foreach my $key (qw(JOURNAL TI PAGES VOL ISSUE SEASON MONTH DAY YEAR)) {
	if ($field_map->{$key} && $data->{$key}) {
	    my $val = $data->{$key};
	    $val =~ s/\\/\\\\/g;
	    $val =~ s/"/\\"/g;
	    push (@where, $field_map{$key} . ' like "' . $val . '"');
	}
    }

    my $sth = $dbh->prepare('SELECT accession FROM cpi_record where '
        . join(" and ", @where) . " limit 1");
    $sth->execute();
    $sth->bind_columns(\$accession);
    $sth->fetch();
    return $accession;
}

sub findISSN {
    my ($dbh, $journal) = @_;

    $journal =~ s/\\/\\\\/g;
    $journal =~ s/"/\\"/g;
    my $sth = $dbh->prepare('SELECT issn,pub_type,peer_review FROM issn WHERE journal LIKE ?');
    $sth->execute($journal);
    #$sth->bind_columns( \( @data{ @{$sth->{NAME_lc} } } ));
    #$sth->fetch();
    return $sth->fetchrow_hashref();
}

