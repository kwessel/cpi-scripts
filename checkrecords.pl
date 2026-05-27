#!/usr/bin/perl

if ($#ARGV != 2) {
    print "Usage: $0 <input_file> <valid_records_file> <error_records_file>\n";
    exit 1;
}

open ($input, $ARGV[0]) or die "Can't read $ARGV[0]: $!\n";

my $valid_output = IO::File->new(">" . $ARGV[1]) or die "Can't write to file $ARGV[1]: $!\n";
my $error_output = IO::File->new(">" . $ARGV[2]) or die "Can't write to file $ARGV[1]: $!\n";

print "Reading records from: $ARGV[0]\nWriting valid records to $ARGV[1]\n\nWriting errors to $ARGV[2]\n\n";

print $valid_output "\$\n";
print $error_output "\$\n";

my $count = 0;
my $valid_count = 0;
my $error_count = 0;
$| = 1;

while (my $record = getNextRecord($input)) {
    print "\rProcessing record: $count" if ($count%100 == 0);
    my $data = parseRecord($record);
    if (isValidRecord($data)) {
	print $valid_output $record . "\$\n" if (!isValidRecord($data));
	$valid_count++;
    }
    else {
	print $error_output $record . "\$\n" if (!isValidRecord($data));
	$error_count++;
    }

    $count++;
}

$valid_output->close();
$error_output->close();
close $input;

print "\rProcessed records: $count\nFound valid records: $valid_count\nFound errors: $error_count\n";
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
    my ($record) = @_;
    my %field;

    foreach my $line (split("\n", $record)) {
	$line =~ s/ +$//;
	my ($key, $val) = split (" ", $line, 2);
	if ($key eq "Accession") {
	    $val =~ s/^no\. //;
	}
	push (@{$field{$key}}, $val) if ($val ne "");
    }

    return \%field;
}

sub isValidRecord {
    my ($data) = @_;

    return (($data->{TYPE}[0] eq "JOURNAL ARTICLE" || $data->{TYPE}[0] eq "REVIEW")
	&& $data->{TI} && $data->{YEAR} && $data->{JOURNAL});
}

