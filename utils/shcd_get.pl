#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;

$ENV{PERL_LWP_ENV_PROXY} = 1
    if ($ENV{http_proxy});

$| = 0;

my $key = shift @ARGV;
my $output_file = shift @ARGV;
my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

die "Usage: $0 <key> </path/to/output_file> [<hosts>]" unless($key && $output_file && $hosts);

my @hosts_array = split(',', $hosts);
my $host = $hosts_array[int(rand(scalar(@hosts_array)))];

print "Using host $host : ";
 
my $request = HTTP::Request->new("GET", "http://$host/$key");

my $ua = LWP::UserAgent->new;
my $response = $ua->request($request);

if ($response->code == 200) {
    my $data = $response->content;

    write_file($output_file, $data);
}
print $response->code . "\n";

