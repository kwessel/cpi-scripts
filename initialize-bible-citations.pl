#!/usr/bin/perl

use DBI;
use Roman;

require("./config.pl");

my @canon = (
    "Genesis",
    "Exodus",
    "Leviticus",
    "Numbers",
    "Deuteronomy",
    "Joshua",
    "Judges",
    "Ruth",
    "1 Samuel",
    "2 Samuel",
    "1 Kings",
    "2 Kings",
    "1 Chronicles",
    "2 Chronicles",
    "Ezra",
    "Nehemiah",
    "Tobit",
    "Judith",
    "Esther",
    "1 Maccabees",
    "2 Maccabees",
    "3 Maccabees",
    "4 Maccabees",
    "Job",
    "Psalms",
    "Prayer of Manasseh",
    "1 Esdras",
    "2 Esdras",
    "3 Esdras",
    "4 Esdras",
    "Proverbs",
    "Ecclesiastes",
    "Song of Solomon",
    "Wisdom of Solomon",
    "Ecclesiasticus",
    "Isaiah",
    "Jeremiah",
    "Lamentations",
    "Baruch",
    "Ezekiel",
    "Daniel",
    "Hosea",
    "Joel",
    "Amos",
    "Obadiah",
    "Jonah",
    "Micah",
    "Nahum",
    "Habakkuk",
    "Zephaniah",
    "Haggai",
    "Zechariah",
    "Malachi",
    "Matthew",
    "Mark",
    "Luke",
    "John",
    "Acts",
    "Romans",
    "1 Corinthians",
    "2 Corinthians",
    "Galatians",
    "Ephesians",
    "Philippians",
    "Colossians",
    "1 Thessalonians",
    "2 Thessalonians",
    "1 Timothy",
    "2 Timothy",
    "Titus",
    "Philemon",
    "Hebrews",
    "James",
    "1 Peter",
    "2 Peter",
    "1 John",
    "2 John",
    "3 John",
    "Jude",
    "Revelation"
);

my $dbconn = dbInit($db_host, $db_name, $db_user, $db_password)
    or die "Cannot connect to $db_host: $DBI::errstr\n";

my $new_scripture_citations=0;
my $new_scripture_instances=0;
my $subject_instances=0;

my $records = getSubjects($dbconn);

my %data;
$records->bind_columns( \( @data{ @{$records->{NAME_lc} } } ));

my $count = 0;
$| = 1;

while ($records->fetch) {
    print "\rConsidering Bible subject instances: $count" if ($count%100 == 0);

    if ($data{keyword} =~ /^Bible\. ([^,]+)(, )?(([1-4])[snrt][tdh])?(, )?(([IVXLC]+)(-([IVXLC]+))?)?(, )?(([0-9]+(-[0-9]+)?))?(--)?/i) {
	my $book;
	$book = $4 . " " if (defined($3));
	$book .= $1;

	if (grep (/^$book$/i, @canon)) {
	    $subject_instances++;
	    my $chapter = arabic($7) if (defined($7) && isroman($7));
	    $chapter .= "-" . arabic($9) if (defined($9) && isroman($9));
	    my $verse = $11;
	    my $c = $book;
	    $c .= " " . $chapter if (defined($chapter));
	    $c .= ":" . $verse if (defined($verse));
	    my $scripture_id = findScripture($dbconn,$c);
	    if (!defined($scripture_id)) {
		$scripture_id = createScripture($dbconn,$c);
		$new_scripture_citations++;
	    }

	    my $scripture_instance_id = findScriptureRef($dbconn,$data{record},$scripture_id);
	    if (!defined($scripture_instance_id)) {
                insertScriptureRef($dbconn,$data{record},$scripture_id);
		$new_scripture_instances++;
            }
	}
    }
    $count++;
}
print "\n\n";


$dbconn->disconnect() if ($dbconn);

print "Subject instances with scripture citations: " . $subject_instances . "\n";
print "Scripture citations added: " . $new_scripture_citations . "\n";
print "Scripture citation instances added: " . $new_scripture_instances . "\n";
exit 0;

sub dbInit {
    my ($host, $db, $user, $pw) = @_;

    my $dsn = "DBI:mysql:database=$db;host=$host";
    my $dbh = DBI->connect($dsn, $user, $pw,
	{ RaiseError => 1, mysql_auto_reconnect => 1 });

    return $dbh;
}

sub getSubjects {
    my ($dbh) = @_;

    my $sth = $dbh->prepare("select record,keyword from subject_instance,subject where subject_id = subject and keyword like 'Bible. %';");
    $sth->execute();

    return $sth;
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
    my ($dbh, $scripture) = @_;

    $sth = $dbh->prepare('INSERT INTO scripture set citation=?');
    $sth->execute($scripture);
    return $dbh->last_insert_id(undef, undef, undef, undef);
}

sub findScriptureRef {
    my ($dbh, $accession, $scripture_id) = @_;
    my $scripture_instance_id;

    my $sth = $dbh->prepare('SELECT scripture_instance_id FROM scripture_instance WHERE record LIKE ? AND scripture LIKE ?');
    $sth->execute($accession, $scripture_id);
    $sth->bind_columns(\$scripture_instance_id);
    $sth->fetch();
    return $scripture_instance_id;
}

sub insertScriptureRef {
    my ($dbh, $accession, $scripture_id) = @_;
    $sth = $dbh->prepare("INSERT INTO scripture_instance (record,scripture) VALUES ($accession,$scripture_id)");
    $sth->execute();
}

