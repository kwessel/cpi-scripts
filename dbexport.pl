#!/usr/bin/perl

use DBI;
use IO::File;
use XML::Writer;

require("./config.pl");

my $dbconn = dbInit($db_host, $db_name, $db_user, $db_password);

if ($#ARGV < 0 || $#ARGV > 1) {
    print "Usage: $0 <output_file> [<updated_since_date>]\nUpdated since date should be of the form: YYYY-MM-DD\n";
    exit 1;
}

if ($ARGV[1] ne "" && $ARGV[1] !~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) {
    print "Updated since date must be of the form: YYYY-MM-DD\n";
    exit 1;
}

if (-f $ARGV[0]) {
    print "$ARGV[0] already exists and I won't overwrite it. Please remove or rename it.\n";
    exit 1;
}

my $output = IO::File->new(">" . $ARGV[0]) or die "Can't write to file $ARGV[0]: $!\n";
binmode($output, ":encoding(UTF-8)");

print "Writing records to: $ARGV[0]\n\n";

my $count = 0;
$| = 1;

my $records = getRecordsToExport($dbconn, $ARGV[1]);
my %data;
$records->bind_columns( \( @data{ @{$records->{NAME_lc} } } ));

print $output "<?xml version=\"1.0\" encoding=\"UTF-8\">\n";

#while (my $data = $records->fetchrow_hashref()) {
while ($records->fetch) {
    print "\rExported records: $count" if ($count%100 == 0);
    my $authors;
    if ($data{type} =~ /REVIEW$/) {
	$authors = getReviewedAuthors($dbconn,$data{accession});
    }
    else {
	$authors = getAuthors($dbconn,$data{accession});
    }

    my $subjects = getSubjects($dbconn,$data{accession});

    my $scripture = getScripture($dbconn,$data{accession});

    my $xmlobj = initXML();
    generateXML($xmlobj, \%data, $authors, $subjects, $scripture);
    $xmlobj->end();

    print $output $xmlobj;

    undef $xmlobj;

    $count++;
}

$output->close();
#markRecordsAsExported($dbconn,$ARGV[1]);
$dbconn->do("INSERT INTO export_history SET export_time=NOW()");
$dbconn->disconnect() if ($dbconn);

print "\rExported records: $count\n";

exit 0;

sub dbInit {
    my ($host, $db, $user, $pw) = @_;

    my $dsn = "DBI:mysql:database=$db;host=$host";
    my $dbh = DBI->connect($dsn, $user, $pw,
	{ RaiseError => 1, mysql_auto_reconnect => 1});
    #DBI->{RowCacheSize}=100;

    return $dbh;
}

sub getRecordsToExport {
    my ($dbh, $on_or_after) = @_;
    #my $date_filter = " AND (updated > last_exported OR last_exported IS NULL)";
    my $date_filter;

    $date_filter = " AND updated >= '$on_or_after'" if (defined($on_or_after) && $on_or_after ne "");

    my $sth = $dbh->prepare("SELECT * FROM cpi_record WHERE NOT deleted $date_filter");
#!    my $sth = $dbh->prepare("SELECT * FROM cpi_record WHERE language like '%;%'");
    $sth->execute();

    return $sth;
}

sub getAuthors {
    my ($dbh, $accession) = @_;
    my $val;
    my $authors;

    my $sth = $dbh->prepare("SELECT name FROM author WHERE record = ?");
    $sth->execute($accession);
    $sth->bind_columns(\$val);

    push (@$authors, $val) while ($sth->fetch);

    return $authors;
}

sub getReviewedAuthors {
    my ($dbh, $accession) = @_;
    my $val;
    my $authors;

    my $sth = $dbh->prepare("SELECT name FROM reviewed_author,reviewed_author_instance WHERE reviewed_author_id = reviewed_author AND record = ?");
    $sth->execute($accession);
    $sth->bind_columns(\$val);

    push (@$authors, $val) while ($sth->fetch);

    return $authors;
}

sub getSubjects {
    my ($dbh, $accession) = @_;
    my $val;
    my $subjects;

    my $sth = $dbh->prepare("SELECT UPPER(keyword) FROM subject,subject_instance WHERE subject_id = subject AND record = ?");
    $sth->execute($accession);
    $sth->bind_columns(\$val);

    push (@$subjects, $val) while ($sth->fetch);

    return $subjects;
}

sub getScripture {
    my ($dbh, $accession) = @_;
    my $val;
    my $scripture;

    my $sth = $dbh->prepare("SELECT UPPER(citation) FROM scripture,scripture_instance WHERE scripture_id = scripture AND record = ?");
    $sth->execute($accession);
    $sth->bind_columns(\$val);

    push (@$scripture, $val) while ($sth->fetch);

    return $scripture;
}

sub markRecordsAsExported {
    my ($dbh, $on_or_after) = @_;
    #my $date_filter = " AND (updated > last_exported OR last_exported IS NULL)";
    my $date_filter;

    $date_filter = " AND updated >= '$on_or_after'" if (defined($on_or_after) && $on_or_after ne "");

    my $sth = $dbh->prepare("UPDATE cpi_record SET last_exported=NOW() WHERE NOT deleted $date_filter");
    $sth->execute();
}

sub initXML {
    my $writer = XML::Writer->new(OUTPUT => 'self', DATA_MODE => 1, DATA_INDENT => 2, ENCODING => 'utf-8');
    #$writer->xmlDecl();

    return $writer;
}

sub generateXML {
    my ($writer, $data, $authors, $subjects, $scripture) = @_;

    my $review = ($data->{type} =~ /REVIEW$/);

    if ($review) {
	if ($data->{media}) {
	    $product_type = $data->{media};
	}
	else {
	    $product_type = "book";
	}
    }

    $writer->startTag("article", "article-type" => $data->{type}, "dtd-version" => "1.2d2");
    $writer->startTag("front");

    if ($data->{journal} || $data->{issn}) {
	$writer->startTag("journal-meta");
	if ($data->{journal}) {
	    $writer->startTag("journal-title-group");
	    $writer->dataElement("journal-title", $data->{journal});
	    $writer->endTag(); # journal-title-group
	}

	if ($data->{issn}) {
	    $writer->dataElement("ISSN", $data->{issn});
	}

	$writer->endTag(); # journal-meta
    }

    $writer->startTag("article-meta");
    $writer->dataElement("article-id", $data->{accession}, "assigning-authority" => "CPI", "pub-id-type" => "index");

    if ($data->{doi}) {
	$writer->dataElement("article-id", $data->{doi}, "pub-id-type" => "doi");
    }

    if (($data->{med} && !$review) || ($data->{peer_review} && $data->{peer_review} == 1)) {
	$writer->startTag("article-categories");

	if ($data->{med} && !$review) {
	    $writer->startTag("subj-group");
	    $writer->dataElement("subject", $data->{med});
	    $writer->endTag(); # subj-group
	}

	if ($data->{peer_review} && $data->{peer_review} == 1) {
	    $writer->startTag("subj-group");
	    $writer->dataElement("subject", "peer-reviewed");
	    $writer->endTag(); # subj-group
	}

	$writer->endTag(); # article-categories
    }

    if ($data->{title}) {
	$writer->startTag("title-group");
	$writer->dataElement("article-title", $data->{title});
	$writer->endTag(); # title-group
    }

    if ($review && $data->{reviewer}) {
	$writer->startTag("contrib-group");
	$writer->startTag("contrib", "contrib-type" => "author");
	$writer->dataElement("string-name", $data->{reviewer});
	$writer->endTag(); # contrib
	$writer->endTag(); # contrib-group
    }
    elsif (!$review && $#{$authors} >= 0) {
	$writer->startTag("contrib-group");
	foreach my $author (@$authors) {
	    $writer->startTag("contrib", "contrib-type" => "author");
	    $writer->dataElement("string-name", $author);
	    $writer->endTag(); # contrib
	}
	$writer->endTag(); # contrib-group
    }

    if ($data->{year} || $data->{season} || $data->{month} || $data->{day}) {
	$writer->startTag("pub-date", "publication-format" => "print", "date-type" => "pub");
	if ($data->{month}) {
	    $writer->dataElement("month", $data->{month});
	}
	elsif ($data->{season}) {
	    $writer->dataElement("season", $data->{season});
	}
	if ($data->{day}) {
	    $writer->dataElement("day", $data->{day});
	}
	if ($data->{year}) {
	    $writer->dataElement("year", $data->{year});
	}
	$writer->endTag(); # pub-date
    }

    if ($data->{volume}) {
	$writer->dataElement("volume", $data->{volume});
    }

    if ($data->{issue}) {
	$writer->dataElement("issue", $data->{issue});
    }

    if ($data->{first_page}) {
	$writer->dataElement("fpage", $data->{first_page});
    }
    if ($data->{last_page}) {
	$writer->dataElement("lpage", $data->{last_page});
    }
    if ($data->{page_range}) {
	$writer->dataElement("page-range", $data->{page_range});
    }

    if ($review) {
	$writer->startTag("product", "product-type" => $product_type);

	if ($data->{title}) {
	    $writer->dataElement("source", $data->{title});
	}

	foreach my $author (@$authors) {
	    $writer->dataElement("string-name", $author);
	}

	$writer->endTag(); # product
    }

    if ($data->{url}) {
	$writer->dataElement("self-uri", $data->{url});
    }

    if ($#{$subjects} >= 0) {
	$writer->startTag("kwd-group", "kwd-group-type" => "lcsh", "xml:lang" => "en");
	foreach my $subj (@$subjects) {
	    $writer->dataElement("kwd", $subj);
	}
	$writer->endTag(); # kwd-group
    }

    if ($#{$scripture} >= 0) {
	$writer->startTag("kwd-group", "kwd-group-type" => "scripture citation", "xml:lang" => "en");
	foreach my $citation (@$scripture) {
	    $writer->dataElement("kwd", $citation);
	}
	$writer->endTag(); # kwd-group
    }

    if ($data->{language}) {
	$writer->startTag("Source_Language");
	foreach my $lang (split (/;/, $data->{language})) {
		$writer->dataElement("Language_Code", $lang);
	}
	$writer->endTag(); # Source_Language
    }

    $writer->endTag(); # article-meta

    if ($data->{illustration}) {
	$writer->startTag("notes");
	$writer->dataElement("p", $data->{illustration});
	$writer->endTag();
    }

    $writer->endTag(); # front
    $writer->endTag(); # article
    return;
}
