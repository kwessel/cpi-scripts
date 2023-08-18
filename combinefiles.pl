#!/usr/bin/perl

if ($#ARGV < 0 || $#ARGV > 1) {
    print "Usage: $0 <output_file> [<directory>]\n";
    print "If <directory> is omitted, the current directory will be used.\n";
    exit 1;
}

my $outfile = shift;

my $dirname = shift;;
$dirname = $dirname ? $dirname : ".";

if (-f $outfile) {
    print "$outfile already exists, not overwriting it\n";
    exit 2;
}

open (OUT, ">$outfile") || die "Can't write to $outfile: $!\n";

opendir (DIR, $dirname) || die "Can't access $dirname: $!\n";

my $filecount = 0;

while (my $infile = readdir(DIR)) {
    next if ($infile eq "." || $infile eq ".." || $infile eq $outfile);

    if (! -f $dirname . "/" . $infile) {
        print "Skipping $infile: not a file\n";
        next;
    }

    open (IN, $dirname . "/" . $infile) || do {
        print "Can't open $infilefor reading: $!\n";
        next;
    };

    my @file_contents = <IN>;
    close IN;
    print OUT join ("", @file_contents) . "\n\n";
    $filecount++;
}

closedir (DIR);
close OUT;

print "Concatinated $filecount files into $outfile\n";
exit 0;

