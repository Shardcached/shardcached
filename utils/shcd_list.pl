#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;

$ENV{PERL_LWP_ENV_PROXY} = 1
    if ($ENV{http_proxy});

my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

my @hosts_array = split(',', $hosts);

foreach my $host (@hosts_array) {
    $host = "http://$host" if ($host !~ /^http:\/\//i);
    $host =~ s/\/+$//;
    print "** Keys on host $host\n\n";
    my $request = HTTP::Request->new("GET", "$host/__index__?nohtml=1");
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    my $data = $response->content;
    my @lines = split("\r\n", $data);
    foreach my $line (@lines) {
        my ($key, $length) = split(';', $line);
        printf("%-64.256s %s\n", $key, $length);
    }
    print "\n";
}

