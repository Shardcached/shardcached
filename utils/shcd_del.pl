#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;
use File::Slurp;
use URI::Escape;

my $key = shift @ARGV;
my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

my @hosts_array = split(',', $hosts);
my $host = $hosts_array[int(rand(scalar(@hosts_array)))];

$host =~ s/^http:\/\///;
$host =~ s/\/+$//;

print "Using host $host : ";

my $path = uri_escape($key);
my $request = HTTP::Request->new("DELETE", "http://$host/$path");

my $ua = LWP::UserAgent->new;
my $response = $ua->request($request);
printf "%s : %s\n",  $response->code, $response->message;


