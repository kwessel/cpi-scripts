#!/usr/bin/perl

use DBI;
use LWP::UserAgent;

my $path = $0;
$path =~ s|[^/]+$||;
$path = "./" . $path if ($path !~ m|^/|);
require("$path/config.pl");

my $bad_links=0;

if ($#ARGV != 0) {
    print "Usage: $0 <output_file>\n";
    exit 1;
}

if (-f $ARGV[0]) {
    print "$ARGV[0] already exists and I won't overwrite it. Please remove or rename it.\n";
    exit 1;
}

my $dbconn = dbInit($db_host, $db_name, $db_user, $db_password)
    or die "Cannot connect to $db_host: $DBI::errstr\n";

my $output = IO::File->new(">" . $ARGV[0]) or die "Can't write to file $ARGV[0]: $!\n";
binmode($output, ":encoding(UTF-8)");
print $output '"Accession","Title","URL"' . "\n";

print "Writing records to: $ARGV[0]\n\n";

my $records = getURLs($dbconn);

my %data;
$records->bind_columns( \( @data{ @{$records->{NAME_lc} } } ));

my $count = 0;
$| = 1;

my $ua = LWP::UserAgent->new(timeout => 3);
while ($records->fetch) {
    #print "\rChecking records with URLs: $count" if ($count%100 == 0);
    print "\rChecking records with URLs: $count";

    #print "checking $data{url}\n";
    my $resp = $ua->get($data{url});
    #print "Response: " . $resp->code . "\n";
    if (!$resp->is_success) {
        print $output '"' . $data{accession} . '","' . $data{title} . '","' . $data{url} . '"' . "\n";
            }

    $count++;
}
print "\n\n";

$dbconn->disconnect() if ($dbconn);
$output->close;

print "Stale links found: " . $bad_links . "\n";
exit 0;

sub dbInit {
    my ($host, $db, $user, $pw) = @_;

    my $dsn = "DBI:mysql:database=$db;host=$host";
    my $dbh = DBI->connect($dsn, $user, $pw,
	{ RaiseError => 1, mysql_auto_reconnect => 1 });

    return $dbh;
}

sub getURLs {
    my ($dbh) = @_;

    my $sth = $dbh->prepare("select accession,title,url from cpi_record where url is not null;");
    $sth->execute();

    return $sth;
}

