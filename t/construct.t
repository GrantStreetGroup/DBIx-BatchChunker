#!/usr/bin/perl

use lib qw(t/lib);
use strict;
use warnings;

use Test2::Bundle::More;
use Test2::Tools::Compare;
use Test2::Tools::Exception;
use Test2::Tools::Explain;

use DBIx::BatchChunker;
use DBIx::Connector::Retry;
use CDTest;

############################################################

my $DEFAULT_SLEEP        = 0.5;
my $NO_SLEEP             = 0;
my $SLEEP_RATIO          = 0.1;
my $EXPLICIT_MIN_SLEEP   = 0.25;
my $NEGATIVE_SLEEP_RATIO = -1.5;

my $schema   = CDTest->init_schema;
my $track_rs = $schema->resultset('Track')->search({ position => 1 });

my $dbh  = $schema->storage->dbh;
my $conn = DBIx::Connector::Retry->new(
    connect_info => [ @{ $schema->storage->connect_info } ],
);

subtest 'Valid construction' => sub {
    try_ok {
        DBIx::BatchChunker->new(
            rs      => $track_rs,
            coderef => sub {},
        )->execute;
    }
    'execute lives even without min/max calculations';
    try_ok {
        DBIx::BatchChunker->new(
            dbi_connector => $conn,
            min_stmt      => ['SELECT 1', undef],
            max_stmt      => ['SELECT 1', { arg => 1 }, 'foobar'],
            stmt          => 'SELECT 1',
        );
    }
    'constructor lives with different types of stmt coersion';
};

subtest 'sleep_ratio construction' => sub {
    my %base_args = (
        rs      => $track_rs,
        coderef => sub {},
    );

    my $bc_default = DBIx::BatchChunker->new(%base_args);
    is($bc_default->sleep,       $DEFAULT_SLEEP, 'sleep defaults to 0.5 without sleep_ratio');
    is($bc_default->sleep_ratio, undef,          'sleep_ratio turned off by default');

    my $bc_ratio = DBIx::BatchChunker->new(%base_args, sleep_ratio => $SLEEP_RATIO);
    is($bc_ratio->sleep,       $NO_SLEEP,    'default sleep not applied when only sleep_ratio is defined');
    is($bc_ratio->sleep_ratio, $SLEEP_RATIO, 'sleep_ratio recorded');

    my $bc_both = DBIx::BatchChunker->new(
        %base_args,
        sleep_ratio => $SLEEP_RATIO,
        sleep       => $EXPLICIT_MIN_SLEEP,
    );
    is($bc_both->sleep,       $EXPLICIT_MIN_SLEEP, 'explicit sleep kept as minimum bound with sleep_ratio');
    is($bc_both->sleep_ratio, $SLEEP_RATIO,        'sleep_ratio recorded alongside explicit sleep');

    like(
        dies {
            DBIx::BatchChunker->new(%base_args, sleep_ratio => $NEGATIVE_SLEEP_RATIO);
        },
        qr/Must be a number greater than or equal to zero .+sleep_ratio/,
        'constructor dies with a negative sleep_ratio',
    );
};

subtest 'Legacy syntax' => sub {
    local $SIG{__WARN__} = sub {};

    try_ok {
        DBIx::BatchChunker->new(
            min_sth => $dbh->prepare('SELECT 1'),
            max_sth => $dbh->prepare('SELECT 1'),
            sth     => $dbh->prepare('SELECT 1'),
        );
    }
    'constructor lives with sth options';

    like(
        dies {
            DBIx::BatchChunker->new(
                min_sth => $dbh->prepare('SELECT 1'),
            );
        },
        qr/Range calculations require/,
        'constructor dies with min_sth + no max_sth',
    );
};

subtest 'Errors' => sub {
    like(
        dies {
            DBIx::BatchChunker->new;
        },
        qr/Range calculations require/,
        'constructor dies with no parameters'
    );
    like(
        dies {
            DBIx::BatchChunker->construct_and_execute;
        },
        qr/Range calculations require/,
        'construct_and_execute dies with no parameters',
    );

    like(
        dies {
            DBIx::BatchChunker->new(
                dbi_connector => $conn,
                min_stmt      => 'SELECT 1',
                stmt          => 'SELECT 1',
            );
        },
        qr/Range calculations require/,
        'constructor dies with min_stmt + no max_stmt',
    );
    like(
        dies {
            DBIx::BatchChunker->new(
                dbi_connector => $conn,
                min_stmt      => ['SELECT 1', [], {arg => 1}],
                max_stmt      => ['SELECT 1'],
                stmt          => 'SELECT 1',
            );
        },
        qr/Must be either an SQL string or an arrayref/,
        'constructor dies with bad $sth args',
    );

    like(
        dies {
            DBIx::BatchChunker->new(
                rs => $track_rs,
            );
        },
        qr/Block execution requires/,
        'constructor dies with rs + no coderef',
    );

    like(
        dies {
            DBIx::BatchChunker->new(
                rs          => $track_rs,
                coderef    => sub {},
                single_row => 1,
            );
        },
        qr/Found unknown attribute.+single_row/,
        'constructor dies with misspelled attr',
    );

};

############################################################

done_testing;
