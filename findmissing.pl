#!/usr/bin/perl

use DBI;
use Roman;

require("./config.pl");

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

if ($#ARGV != 1) {
    print "Usage: $0 <input_file> <missing_records_output_file>\n";
    exit 1;
}

open ($input, $ARGV[0]) or die "Can't read $ARGV[0]: $!\n";

if (-f $ARGV[1]) {
    print "$ARGV[1] already exists and I won't overwrite it. Please remove or rename it.\n";
    exit 1;
}

open ($output, ">".$ARGV[1]) or die "Can't write to $ARGV[1]: $!\n";

print "Reading records from: $ARGV[0]\n\n";

my $count = 0;
my $wrote_count = 0;
$| = 1;

while (my $record = getNextRecord($input)) {
    print "\rRead records: $count" if ($count%10 == 0);
    my $data = parseRecord($record, \%field_map);

    if (isValidRecord($data) && !findRecord($dbconn, $data, \%field_map)) {
        print $output "\$\n" if ($wrote_count == 0);
        print $output $record . "\$\n";
	$wrote_count++;
    }
    
    undef $data;

    $count++ if ($record ne "");
}

$dbconn->disconnect() if ($dbconn);
close $input;
close $output;
unlink $ARGV[1] if ($wrote_count == 0);

print "\rRead records: $count\nFound missing records: $wrote_count\n";

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

