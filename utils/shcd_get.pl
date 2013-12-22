#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;
use URI::Escape;

$ENV{PERL_LWP_ENV_PROXY} = 1
    if ($ENV{http_proxy});

$| = 0;

my $key = shift @ARGV;
my $output_file = shift @ARGV;
my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

$output_file = $key if !$output_file;

die "Usage: $0 <key> </path/to/output_file> [<hosts>]" unless($key && $output_file && $hosts);

my @hosts_array = split(',', $hosts);
my $host = $hosts_array[int(rand(scalar(@hosts_array)))];

$host =~ s/^http:\/\///;
$host =~ s/\/+$//;

print "Using host $host : ";
 
my $path = uri_escape($key);

my $request = HTTP::Request->new("GET", "http://$host/$path");

my $ua = LWP::UserAgent->new;
my $response = $ua->request($request);

if ($response->code == 200) {
    my $data = $response->content;
    print "Saving to: $output_file\n";
    write_file($output_file, $data);
}
print $response->code . "\n";

