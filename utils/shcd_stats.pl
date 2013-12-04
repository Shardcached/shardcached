#!/usr/bin/env perl

use strict;
use HTTP::Request;
use LWP;

$ENV{PERL_LWP_ENV_PROXY} = 1
    if ($ENV{http_proxy});

my $hosts = shift @ARGV || $ENV{SHCD_HOSTS};

my @hosts_array = split(',', $hosts);

foreach my $host (@hosts_array) {
    print "** Counters from host $host\n\n";
    my $request = HTTP::Request->new("GET", "http://$host/__stats__?nohtml=1");
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    my $data = $response->content;
    my @lines = split("\r\n", $data);
    foreach my $line (@lines) {
        my ($key, $value) = split(';', $line);
        next if ($key =~ /^worker\[/ && $value == 0);
        printf("%-64.256s %s\n", $key, $value);
    }
    print "\n";
}

