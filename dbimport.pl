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
    'PEER_REVIEWED' => 'peer_reviewed',
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

if ($#ARGV != 0) {
    print "Usage: $0 <input_file>\n";
    exit 1;
}

open ($input, $ARGV[0]) or die "Can't read $ARGV[0]: $!\n";

print "Reading records from: $ARGV[0]\n\n";

my $issn_data;
my @journals_not_found;

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
	    push (@journals_not_found, $journal);
	}
    }
}

close $input;

if ($#journals_not_found >= 0) {
    print "The following " . $#journals_not_found+1 . " journals are missing entries in the ISSN ttable:\n";
    print join("\n", @journals_not_found) . "\n";

    $dbconn->disconnect() if ($dbconn);
    close $input;
    exit 1;
}

print "Found " . keys(%$issn_data) . " in the input.\n\n";

open ($input, $ARGV[0]) or die "Can't read $ARGV[0]: $!\n";

my $count = 0;
my $wrote_count = 0;
my $new_reviewed_authors=0;
my $new_subjects=0;
$| = 1;

while (my $record = getNextRecord($input)) {
    print "\rRead records: $count" if ($count%10 == 0);
    my $data = parseRecord($record, \%field_map);

    if (isValidRecord($data)) {
	$data->{ISSN} = $issn_data->{$data->{JOURNAL}}->{issn} if (!$data->{ISSN});

	my $type = $data->{TYPE};
	$data->{TYPE} = $issn_data->{$data->{JOURNAL}}->{pub_type} . " " . $type;

	$data->{PEER_REVIEWED} = $issn_data->{$data->{JOURNAL}}->{peer_reviewed};

	my $accession = insertRecord($dbconn, $data, \%field_map);

	if ($data->{TYPE} eq "JOURNAL ARTICLE") {
	    insertArticleAuthors($dbconn,$accession,$data->{AU});
	}
	elsif ($data->{TYPE} eq "REVIEW") {
	    my $author_ids;

	    foreach my $a (@{$data->{AU}}) {
		my $author_id = findReviewedAuthor($dbconn,$a);
		if (!defined($author_id)) {
		    $author_id = createReviewedAuthor($dbconn,$a);
		    $new_reviewed_authors++;
		}
		push (@$author_ids, $author_id);
	    }

	    insertReviewedAuthors($dbconn,$accession,$author_ids);
	}

	my $subject_ids;
	my $scripture_ids;
	foreach my $s (@{$data->{SUB}}) {
	    my $subject_id = findSubject($dbconn,$s);
	    if (!defined($subject_id)) {
		$subject_id = createSubject($dbconn,$s);
		$new_subjects++;
	    }
	    push (@$subject_ids, $subject_id);

	    if ($foo =~ /^Bible\. ([^,]+)(, )?(([IVXLC]+)(-([IVXLC]+))?)?(, )?([0-9-]+)?(--)?/) {
		my $book = $1;
		my $chapter = arabic($4) if (defined($4) && isroman($4));
		my $chapter .= "-" . arabic($6) if (defined($6) && isroman($6));
		my $verse = $8;
		my $c = $book;
		$c .= " " . $chapter if (defined($chapter));
		$c .= ":" . $verse if (defined($verse));
		my $scripture_id = findScripture($dbconn,$c);
		if (!defined($scripture_id)) {
		    $scripture_id = createScripture($dbconn,$c);
		    $new_scripture_refs++;
		}
		push (@$scripture_ids, $scripture_id);
	    }
	}

	insertSubjects($dbconn,$accession,$subject_ids);
	insertScriptureRefs($dbconn,$accession,$scripture_ids);

	$wrote_count++;
    }
    
    undef $data;

    $count++;
}

$dbconn->disconnect() if ($dbconn);
close $input;

print "\rRead records: $count\nAdded records: $wrote_count\n";

print "Subjects added: " . $new_subjects . "\n";
print "Scripture citations added: " . $new_scripture_refs . "\n";
print "Reviewed authors added: " . $new_reviewed_authors . "\n";

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

    return (($data->{TYPE} eq "JOURNAL ARTICLE" || $data->{TYPE} eq "REVIEW")
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

sub insertRecord {
    my ($dbh, $data, $field_map) = @_;
    my @cols;
    my @vals;

    foreach my $key (keys %$data) {
	if ($field_map->{$key} && $field_map->{$key} ne null) {
	    push (@cols, $field_map{$key});
	    my $val = $data->{$key};
	    $val =~ s/\\/\\\\/g;
	    $val =~ s/"/\\"/g;
	    push (@vals, '"' . $val . '"');
	}
    }

    my $sth = $dbh->prepare('INSERT INTO cpi_record (' . join(",", @cols) .
	') VALUES (' . join (",", @vals) . ')');
    $sth->execute();
    return $dbh->last_insert_id(undef, undef, undef, undef);
}

sub insertArticleAuthors {
    my ($dbh, $accession, $authors) = @_;
    my @inserts;

    foreach my $author (@$authors) {
	$author =~ s/\\/\\\\/g;
	$author =~ s/"/\\"/g;
	push (@inserts, '(' . $accession . ',"' . $author . '")');
    }

    if ($#inserts >= 0) {
	my $sth = $dbh->prepare('INSERT INTO author (record,name) VALUES ' .
	    join(',',@inserts));
	$sth->execute();
    }
}

sub findReviewedAuthor {
    my ($dbh, $author) = @_;
    my $author_id;

    $author =~ s/\\/\\\\/g;
    $author =~ s/"/\\"/g;
    my $sth = $dbh->prepare('SELECT reviewed_author_id FROM reviewed_author WHERE name LIKE ?');
    $sth->execute($author);
    $sth->bind_columns(\$author_id);
    $sth->fetch();
    return $author_id;
}

sub createReviewedAuthor {
    my ($dbh, $author) = @_;

    $author =~ s/\\/\\\\/g;
    $author =~ s/"/\\"/g;
    $sth = $dbh->prepare('INSERT INTO reviewed_author set name=?');
    $sth->execute($author);
    return $dbh->last_insert_id(undef, undef, undef, undef);
}

sub insertReviewedAuthors {
    my ($dbh, $accession, $author_ids) = @_;
    my @inserts;

    foreach my $author_id (@$author_ids) {
	push (@inserts, '(' . $accession . ',' . $author_id . ')');
    }

    if ($#inserts >= 0) {
	$sth = $dbh->prepare('INSERT INTO reviewed_author_instance (record,reviewed_author) VALUES ' .
	    join(',',@inserts));
	$sth->execute();
    }
}

sub findSubject {
    my ($dbh, $subject) = @_;
    my $subject_id;

    $subject =~ s/\\/\\\\/g;
    $subject =~ s/"/\\"/g;
    my $sth = $dbh->prepare('SELECT subject_id FROM subject WHERE keyword LIKE ?');
    $sth->execute($subject);
    $sth->bind_columns(\$subject_id);
    $sth->fetch();
    return $subject_id;
}

sub createSubject {
    my ($dbh, $subject) = @_;

    $subject =~ s/\\/\\\\/g;
    $subject =~ s/"/\\"/g;
    $sth = $dbh->prepare('INSERT INTO subject set keyword=?');
    $sth->execute($subject);
    return $dbh->last_insert_id(undef, undef, undef, undef);
}

sub insertSubjects {
    my ($dbh, $accession, $subject_ids) = @_;
    my @inserts;

    foreach my $subject_id (@$subject_ids) {
	push (@inserts, '(' . $accession . ',' . $subject_id . ')');
    }

    if ($#inserts >= 0) {
	$sth = $dbh->prepare('INSERT INTO subject_instance (record,subject) VALUES ' .
	    join(',',@inserts));
	$sth->execute();
    }
}

sub findScripture {
    my ($dbh, $scripture) = @_;
    my $scripture_id;

    my $sth = $dbh->prepare('SELECT scripture_id FROM scripture WHERE citation LIKE ?');
    $sth->execute($scripture);
    $sth->bind_columns(\$scripture_id);
    $sth->fetch();
    return $scripture_id;
}

sub createScripture {
    my ($dbh, $Scripture) = @_;

    $sth = $dbh->prepare('INSERT INTO scripture set citation=?');
    $sth->execute($scripture);
    return $dbh->last_insert_id(undef, undef, undef, undef);
}

sub insertScriptureRefs {
    my ($dbh, $accession, $scripture_ids) = @_;
    my @inserts;

    foreach my $scripture_id (@$scripture_ids) {
	push (@inserts, '(' . $accession . ',' . $scripture_id . ')');
    }

    if ($#inserts >= 0) {
	$sth = $dbh->prepare('INSERT INTO scripture_instance (record,scripture) VALUES ' .
	    join(',',@inserts));
	$sth->execute();
    }
}

sub findISSN {
    my ($dbh, $journal) = @_;

    $journal =~ s/\\/\\\\/g;
    $journal =~ s/"/\\"/g;
    my $sth = $dbh->prepare('SELECT issn,pub_type,peer_reviewed FROM issn WHERE journal LIKE ?');
    $sth->execute($journal);
    #$sth->bind_columns( \( @data{ @{$sth->{NAME_lc} } } ));
    #$sth->fetch();
    return $sth->fetchrow_hashref();
}

