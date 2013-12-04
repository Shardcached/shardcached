#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;

$ENV{PERL_LWP_ENV_PROXY} = 1
    if ($ENV{http_proxy});

my $input_file = shift @ARGV;
my $key = shift @ARGV;
my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

die "Usage: $0 </path/to/input_file> <key> [<hosts>]" unless($key && $input_file && $hosts);

my @hosts_array = split(',', $hosts);
my $host = $hosts_array[int(rand(scalar(@hosts_array)))];

print "Using host $host : ";
 
my $data = read_file($input_file);
my $req_headers = HTTP::Headers->new( 'Content-Length' => length($data) );
my $request = HTTP::Request->new("PUT", "http://$host/$key", $req_headers, $data);

my $ua = LWP::UserAgent->new;
my $response = $ua->request($request);
printf "%s : %s\n",  $response->code, $response->message;


