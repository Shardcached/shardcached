#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;
use DBI;

my $dbfile = $ENV{SHCD_DBFILE} || "$ENV{HOME}/shd.db";

my $input_file = shift @ARGV;
my $key = shift @ARGV;
my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

die "Usage: $0 </path/to/input_file> <key> [<hosts>]" unless($key && $input_file && $hosts);

my @hosts_array = split(',', $hosts);
my $host = $hosts_array[int(rand(scalar(@hosts_array)))];

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","")
    or die "Can't open sqlite database $dbfile";

$dbh->do("CREATE TABLE IF NOT EXISTS shd_index (key TEXT PRIMARY KEY, size INTEGER)");


print "Using host $host : ";
 
my $data = read_file($input_file);
my $req_headers = HTTP::Headers->new( 'Content-Length' => length($data) );
my $request = HTTP::Request->new("PUT", "http://$host/$key", $req_headers, $data);

my $ua = LWP::UserAgent->new;
my $response = $ua->request($request);
if ($response->code == 200) {
    $dbh->do(sprintf"INSERT OR REPLACE INTO shd_index VALUES(%s, %d)", $dbh->quote($key), length($data));
}
printf "%s : %s\n",  $response->code, $response->message;


