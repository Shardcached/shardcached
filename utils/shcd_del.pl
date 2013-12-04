#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;
use DBI;

my $dbfile = "$ENV{HOME}/shd.db";

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","")
    or die "Can't open sqlite database $dbfile";

$dbh->do("CREATE TABLE IF NOT EXISTS shd_index (key TEXT PRIMARY KEY, size INTEGER)");

my $key = shift @ARGV;
my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

my @hosts_array = split(',', $hosts);
my $host = $hosts_array[int(rand(scalar(@hosts_array)))];

$host = "http://$host" if ($host !~ /^http:\/\//i);
$host =~ s/\/+$//;

print "Using host $host : ";

my $request = HTTP::Request->new("DELETE", "$host/$key");

my $ua = LWP::UserAgent->new;
my $response = $ua->request($request);
if ($response->code == 200) {
    $dbh->do(sprintf"DELETE FROM shd_index WHERE key=%s", $dbh->quote($key));
}
printf "%s : %s\n",  $response->code, $response->message;


