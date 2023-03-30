#!/usr/bin/perl

use strict;
use warnings;

use Test2::Bundle::More;
use Test2::Require::AuthorTesting;
use Test::Pod::Coverage 0.08;

my %global_exceptions = ( also_private => [qw< BUILDARGS FOREIGNBUILDARGS BUILD DEMOLISH >] );

my %exceptions = (
    'DBIx::BatchChunker'            => { trustme => [qw/^(debug)$/] },
    'DBIx::BatchChunker::LoopState' => { trustme => [qw/^(timer)$/] },
);

my @modules = all_modules();

plan tests => scalar @modules;

foreach my $module (@modules) {
    my %exceptions = ( %global_exceptions, %{ $exceptions{$module} || {} } );
    pod_coverage_ok($module, \%exceptions);
}

done_testing;
