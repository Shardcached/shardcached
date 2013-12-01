#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;
use DBI;

my $dbfile = "$ENV{HOME}/shd.db";

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","")
    or die "Can't open sqlite database $dbfile";

$dbh->do("CREATE TABLE IF NOT EXISTS shd_index (key TEXT PRIMARY KEY)");
my $ary_ref  = $dbh->selectcol_arrayref("SELECT * from shd_index");

foreach my $key (@$ary_ref) {
    print "$key\n";
}

