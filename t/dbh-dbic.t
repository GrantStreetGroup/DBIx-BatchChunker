#!/usr/bin/perl

use lib qw(t/lib);
use strict;
use warnings;

use Test2::Bundle::More;
use Test2::Tools::Compare;
use Test2::Tools::Exception;
use Test2::Tools::Explain;

use POSIX        qw( ceil );
use Scalar::Util qw( looks_like_number );
use Time::HiRes  qw( time );
use Env          qw( BATCHCHUNK_TEST_DEBUG );

use DBIx::BatchChunker;
use CDTest;

############################################################

my $CHUNK_SIZE = 3;

my $schema       = CDTest->init_schema;
my $track_rs     = $schema->resultset('Track')->search({ position => 1 });
my $track1_count = $track_rs->count;

my $dbh = $schema->storage->dbh;

subtest 'DBIC Processing (+ process_past_max)' => sub {
    my $calls = 0;

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        rs          => $track_rs,
        coderef     => sub {
            my ($bc, $rs) = @_;
            isa_ok($rs, ['DBIx::Class::ResultSet'], '$rs');
            $calls++;
            note explain $bc->_loop_state if $BATCHCHUNK_TEST_DEBUG;
        },

        process_past_max  => 1,
        min_chunk_percent => 0,
    );

    is($batch_chunker->id_name, 'me.trackid', 'Right id_name guessed');
    isa_ok($batch_chunker->rsc, ['DBIx::Class::ResultSetColumn'], '$rsc');

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    my $range = $batch_chunker->max_id - $batch_chunker->min_id + 1;

    # Process

    # NOTE: If the last remaining chunk is exactly the size of chunk_size, the
    # process_past_max code will process one more chunk.  If that chunk is short,
    # it'll use that chunk immediately to the PPM point.  Thus, the +1 here is
    # before the division.
    my $right_calls = ceil( ($range + 1) / $CHUNK_SIZE);
    $batch_chunker->execute;
    cmp_ok($calls, '==', $right_calls, 'Right number of calls');
};

subtest 'DBIC Processing + single_rows (+ rsc)' => sub {
    my $calls = 0;

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        rsc         => $track_rs->get_column('trackid'),
        rs          => $track_rs,
        coderef     => sub {
            my ($bc, $result) = @_;
            isa_ok($result, ['DBIx::Class::Row'], '$result');
            $calls++;
            note explain $bc->_loop_state if $BATCHCHUNK_TEST_DEBUG;
        },

        single_rows       => 1,
        min_chunk_percent => 0,
    );

    is($batch_chunker->id_name, 'me.trackid', 'Right id_name guessed and aliased');

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    # Process
    $batch_chunker->execute;
    cmp_ok($calls, '==', $track1_count, 'Right number of calls');
};

subtest 'Active DBI Processing (+ sleep)' => sub {
    my $calls = 0;

    # can't exactly make it an "active" statement, but we can add a callback
    my $sth = $dbh->prepare('SELECT ?, ?');
    $sth->{Callbacks} = {
        execute => sub { $calls++; return },  # DBI callback cannot return anything
    };

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        min_sth => $dbh->prepare('SELECT MIN(trackid) FROM track WHERE position = 1'),
        max_sth => $dbh->prepare('SELECT MAX(trackid) FROM track WHERE position = 1'),
        sth     => $sth,

        sleep => 0.1,
    );

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    my $range = $batch_chunker->max_id - $batch_chunker->min_id + 1;
    my $multiplier_range = ceil($range / $CHUNK_SIZE);

    # Process
    my $start_time = time;
    $batch_chunker->execute;
    my $total_time = time - $start_time;
    cmp_ok($calls,      '==', $multiplier_range,       'Right number of calls');
    cmp_ok($total_time, '>=', $multiplier_range * 0.1, 'Slept ok');
    cmp_ok($total_time, '<',  $multiplier_range * 0.5, 'Did not oversleep');
};

subtest 'Query DBI Processing (+ min_chunk_percent)' => sub {
    my $calls     = 0;
    my $max_range = 0;

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        min_sth   => $dbh->prepare('SELECT MIN(trackid)   FROM track WHERE position = 1'),
        max_sth   => $dbh->prepare('SELECT MAX(trackid)   FROM track WHERE position = 1'),
        sth       => $dbh->prepare('SELECT trackid        FROM track WHERE position = 1 AND trackid BETWEEN ? AND ?'),
        count_sth => $dbh->prepare('SELECT COUNT(trackid) FROM track WHERE position = 1 AND trackid BETWEEN ? AND ?'),
        coderef   => sub {
            my ($bc, $sth) = @_;
            isa_ok($sth, ['DBI::st'], '$sth');
            $calls++;

            my $ls     = $bc->_loop_state;
            my $range  = $ls->{end} - $ls->{start} + 1;
            $max_range = $range if $range > $max_range;
            note explain $ls if $BATCHCHUNK_TEST_DEBUG;
        },

        # any missing row in a standard sized chunk will trigger an expansion
        min_chunk_percent => sprintf("%.2f",
            ($CHUNK_SIZE - 1) / $CHUNK_SIZE
        ) + 0.01,
    );

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    my $range = $batch_chunker->max_id - $batch_chunker->min_id + 1;
    my $multiplier_range = ceil($range / $CHUNK_SIZE);

    # Process
    $batch_chunker->execute;
    cmp_ok($calls,      '<', $multiplier_range, 'Fewer coderef calls than normal');
    cmp_ok($max_range,  '>', $CHUNK_SIZE,       'Expanded chunk at least once');
};

subtest 'Query DBI Processing + single_row (+ rsc)' => sub {
    my $calls = 0;

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        ### NOTE: This mixing of DBI/C is unconventional, but still acceptable
        rsc       => $track_rs->get_column('trackid'),
        sth       => $dbh->prepare('SELECT *              FROM track WHERE position = 1 AND trackid BETWEEN ? AND ?'),
        count_sth => $dbh->prepare('SELECT COUNT(trackid) FROM track WHERE position = 1 AND trackid BETWEEN ? AND ?'),

        coderef => sub {
            my ($bc, $row) = @_;
            like($row, {
                trackid  => qr/[0-9]+/,
                cd       => qr/[0-9]+/,
                position => 1,
                title    => qr/\w+/,
            }, '$row + keys');
            $calls++;

            if ($BATCHCHUNK_TEST_DEBUG) {
                note explain $bc->_loop_state;
                note explain $row;
            }
        },

        single_rows => 1,
    );

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    # Process
    $batch_chunker->execute;
    cmp_ok($calls, '==', $track1_count, 'Right number of calls');
};

subtest 'DIY Processing (+ process_past_max)' => sub {
    my $calls = 0;
    my $max_range = 0;

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        rsc     => $track_rs->get_column('trackid'),
        coderef => sub {
            my ($bc, $start, $end) = @_;
            ok(looks_like_number $start,  '$start is a number');
            ok(looks_like_number $end,    '$end   is a number');
            $calls++;

            note explain { start => $start, end => $end } if $BATCHCHUNK_TEST_DEBUG;
        },

        process_past_max => 1,
    );

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    my $range = $batch_chunker->max_id - $batch_chunker->min_id + 1;

    # Process
    my $right_calls = ceil( ($range + 1) / $CHUNK_SIZE);  # see PPM note on the first subtest
    $batch_chunker->execute;
    cmp_ok($calls, '==', $right_calls, 'Right number of calls');
};

subtest 'process_past_max + min_chunk_percent' => sub {
    my $calls     = 0;
    my $max_count = 0;
    my $max_id    = 0;

    # Constructor
    my $batch_chunker = DBIx::BatchChunker->new(
        chunk_size => $CHUNK_SIZE,

        rs          => $track_rs,
        coderef     => sub {
            my ($bc, $rs) = @_;
            isa_ok($rs, ['DBIx::Class::ResultSet'], '$rs');
            $calls++;

            my $ls     = $bc->_loop_state;
            $max_count = $ls->{chunk_count} if $ls->{chunk_count} > $max_count;
            $max_id    = $ls->{end}         if $ls->{end}         > $max_id;

            note explain $ls if $BATCHCHUNK_TEST_DEBUG;
        },

        process_past_max  => 1,
        # any missing row in a standard sized chunk will trigger an expansion
        min_chunk_percent => sprintf("%.2f",
            ($CHUNK_SIZE - 1) / $CHUNK_SIZE
        ) + 0.01,
    );

    # Calculate
    ok($batch_chunker->calculate_ranges, 'calculate_ranges ok');
    ok($batch_chunker->min_id,           'min_id ok');
    ok($batch_chunker->max_id,           'max_id ok');

    my $range       = $batch_chunker->max_id - $batch_chunker->min_id + 1;
    my $real_max_id = $batch_chunker->max_id;
    my $multiplier_range = ceil($range / $CHUNK_SIZE);

    # Now, sabotage the max_id, so that process_past_max has to work through multiple chunks
    $batch_chunker->max_id(
        int($range / 2) + $batch_chunker->min_id
    );

    # Process
    my $max_chunk_count = $CHUNK_SIZE * 2 - 1;
    my $max_chunk_calls = ceil($track1_count / $max_chunk_count);

    $batch_chunker->execute;
    cmp_ok($calls,      '<',  $multiplier_range, 'Fewer coderef calls than normal');
    cmp_ok($calls,      '>=', $max_chunk_calls,  'More coderef calls than minimum threshold');
    cmp_ok($max_count,  '<=', $max_chunk_count,  'Did not exceed max chunk percentage');
    cmp_ok($max_id,     '>=', $real_max_id,      'Looked at all of the IDs');
};

# Verify that the automatic constructor correctly constructs the object,
# calculates the ranges, and executes
subtest 'Automatic execution (DBIC Processing + single_rows + rsc)' => sub {
    my $calls = 0;

    my $batch_chunker = DBIx::BatchChunker->construct_and_execute(
        chunk_size => $CHUNK_SIZE,

        rsc         => $track_rs->get_column('trackid'),
        rs          => $track_rs,
        coderef     => sub {
            my ($bc, $result) = @_;
            isa_ok($result, ['DBIx::Class::Row'], '$result');
            $calls++;
            note explain $bc->_loop_state if $BATCHCHUNK_TEST_DEBUG;
        },

        single_rows       => 1,
        min_chunk_percent => 0,
    );

    isa_ok($batch_chunker, ['DBIx::BatchChunker'], '$bc');
    cmp_ok($calls, '==', $track1_count, 'Right number of calls');
};

subtest 'Errors' => sub {
    like(
        dies {
            DBIx::BatchChunker->new->calculate_ranges;
        },
        qr/Need at least a/,
        'calculate_ranges dies with no parameters'
    );

    like(
        dies {
            DBIx::BatchChunker->new(
                min_sth => $dbh->prepare('SELECT 1'),
            )->calculate_ranges;
        },
        qr/Need at least a/,
        'calculate_ranges dies with min_sth + no max_sth',
    );

    like(
        dies {
            DBIx::BatchChunker->new->execute;
        },
        qr/Need at least a/,
        'execute dies with no parameters',
    );

    like(
        dies {
            DBIx::BatchChunker->new(
                rs => $track_rs,
            )->execute;
        },
        qr/Need at least a/,
        'execute dies with rs + no coderef',
    );

    ok(
        lives {
            DBIx::BatchChunker->new(
                rs      => $track_rs,
                coderef => sub {},
            )->execute;
        },
        'execute lives even without min/max calculations',
    );

    like(
        dies {
            DBIx::BatchChunker->construct_and_execute;
        },
        qr/Need at least a/,
        'construct_and_execute dies with no parameters',
    );
};

############################################################

done_testing;
