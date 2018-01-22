# NAME

DBIx::BatchChunker - Run large database changes safely

# VERSION

version 0.90

# SYNOPSIS

```perl
use DBIx::BatchChunker;

my $account_rs = $schema->resultset('Account')->search({
    account_type => 'deprecated',
});

my %params = (
    chunk_size  => 5000,
    target_time => 15,

    rs      => $account_rs,
    id_name => 'account_id',

    coderef => sub { $_[1]->delete },
    sleep   => 1,
    debug   => 1,

    process_name     => 'Deleting deprecated accounts',
    process_past_max => 1,
);

# EITHER:
# 1) Automatically construct and execute the changes:

DBIx::BatchChunker->construct_and_execute(%params);

# OR
# 2) Manually construct and execute the changes:

my $batch_chunker = DBIx::BatchChunker->new(%params);

$batch_chunker->calculate_ranges;
$batch_chunker->execute;
```

# DESCRIPTION

This utility class is for running a large batch of DB changes in a manner that doesn't
cause huge locks, outages, and missed transactions.  It's highly flexible to allow for
many different kinds of change operations, and dynamically adjusts chunks to its
workload.

It works by splitting up DB operations into smaller chunks within a loop.  These chunks
are transactionalized, either naturally as single-operation bulk work or by the loop
itself.  The full range is calculated beforehand to get the right start/end points.
A [progress bar](#progress-bar-attributes) will be created to let the deployer know the
processing status.

There are two ways to use this class: call the automatic constructor and executor
(["construct\_and\_execute"](#construct_and_execute)) or manually construct the object and call its methods. See
["SYNOPSIS"](#synopsis) for examples of both.

**DISCLAIMER:** You should not rely on this class to magically fix any and all locking
problems the DB might experience just because it's being used.  Thorough testing and
best practices are still required.

## Processing Modes

This class has several different modes of operation, depending on what was passed to
the constructor:

### DBIC Processing

If both ["rs"](#rs) and ["coderef"](#coderef) are passed, a chunk ResultSet is built from the base
ResultSet, to add in a `BETWEEN` clause, and the new ResultSet is passed into the
coderef.  The coderef should run some sort of active ResultSet operation from there.

An ["id\_name"](#id_name) should be provided, but if it is missing it will be looked up based on
the primary key of the ResultSource.

If ["single\_rows"](#single_rows) is also enabled, then each chunk is wrapped in a transaction and the
coderef is called for each row in the chunk.  In this case, the coderef is passed a
Result object instead of the chunk ResultSet.

### Active DBI Processing

If an ["sth"](#sth) (DBI statement handle object) is passed without a ["coderef"](#coderef), the statement
handle is merely executed on each iteration with the start and end IDs.  It is assumed
that the SQL for the statement handle contains exactly two placeholders for a `BETWEEN`
clause.  For example:

```perl
my $update_sth = $dbh->prepare_cached(q{
UPDATE
    accounts a
    JOIN account_updates au USING (account_id)
SET
    a.time_stamp = au.time_stamp
WHERE
    a.account_id BETWEEN ? AND ? AND
    a.time_stamp != au.time_stamp
});
```

The `BETWEEN` clause should, of course, match the IDs being used in the loop.

### Query DBI Processing

If both a ["sth"](#sth) and a ["coderef"](#coderef) are passed, the statement handle is executed.  Like
the ["Active DBI Processing"](#active-dbi-processing) mode, the SQL for the statement handle should contain
exactly two placeholders for a `BETWEEN` clause.  Then the `$sth` is passed to the
coderef.  It's up to the coderef to extract data from the executed statement handle, and
do something with it.

If `single_rows` is enabled, each chunk is wrapped in a transaction and the coderef is
called for each row in the chunk.  In this case, the coderef is passed a hashref of the
row instead of the executed `$sth`, with lowercase alias names used as keys.

### DIY Processing

If a ["coderef"](#coderef) is passed but neither a `sth` nor a `rs` are passed, then the
multiplier loop does not touch the database.  The coderef is merely passed the start and
end IDs for each chunk.  It is expected that the coderef will run through all database
operations using those start and end points.

### TL;DR Version

```perl
$sth                             = Active DBI Processing
$sth + $coderef                  = Query DBI Processing  | $bc->$coderef($executed_sth)
$sth + $coderef + single_rows=>1 = Query DBI Processing  | $bc->$coderef($row_hashref)
$rs  + $coderef                  = DBIC Processing       | $bc->$coderef($chunk_rs)
$rs  + $coderef + single_rows=>1 = DBIC Processing       | $bc->$coderef($result)
       $coderef                  = DIY Processing        | $bc->$coderef($start, $end)
```

# ATTRIBUTES

See the ["METHODS"](#methods) section for more in-depth descriptions of these attributes and their
usage.

## DBIC Processing Attributes

### rs

A [DBIx::Class::ResultSet](https://metacpan.org/pod/DBIx::Class::ResultSet). This is used by all methods as the base ResultSet onto which
the DB changes will be applied.  Required for DBIC processing.

### rsc

A [DBIx::Class::ResultSetColumn](https://metacpan.org/pod/DBIx::Class::ResultSetColumn). This is only used to override ["rs"](#rs) for min/max
calculations.  Optional.

## DBI Processing Attributes

### min\_sth

### max\_sth

[DBI](https://metacpan.org/pod/DBI) statement handles.  When executed, these statements should each return a single
value (to be used by ["fetchrow\_array" in DBI](https://metacpan.org/pod/DBI#fetchrow_array)), either the minimum or maximum ID that will be
affected by the DB changes.  These are used by ["calculate\_ranges"](#calculate_ranges).  Required if using
either type of DBI Processing.

### sth

If using ["Active DBI Processing"](#active-dbi-processing) (no coderef), this is a [do-able](https://metacpan.org/pod/DBI#do) statement
handle (usually DML like `INSERT/UPDATE/DELETE`).  If using ["Query DBI Processing"](#query-dbi-processing)
(with coderef), this is a passive DQL (`SELECT`) statement handle.

In either case, the statement should contain `BETWEEN` placeholders (one for the
beginning and one for the end of the range), as it will be executed with the start/end ID
points.

Required for DBI Processing.

### count\_sth

A `SELECT COUNT` statement handle.  Like ["sth"](#sth), it should contain `BETWEEN`
placeholders.  In fact, the SQL should look exactly like the ["sth"](#sth) query, except with
`COUNT(*)` instead of the column list.

Used only for ["Query DBI Processing"](#query-dbi-processing).  Optional, but recommended for
[chunk resizing](#min_chunk_percent).

## Progress Bar Attributes

### progress\_bar

The progress bar used for all methods.  This can be specified right before the method
call to override the default used for that method.  Unlike most attributes, this one
is read-write, so it can be switched on-the-fly.

Don't forget to remove or switch to a different progress bar if you want to use a
different one for another method:

```
$batch_chunker->progress_bar( $calc_pb );
$batch_chunker->calculate_ranges;
$batch_chunker->progress_bar( $loop_pb );
$batch_chunker->execute;
```

All of this is optional.  If the progress bar isn't specified, the method will create
a default one.  If the terminal isn't interactive, the default [Term::ProgressBar](https://metacpan.org/pod/Term::ProgressBar) will
be set to `silent` to naturally skip the output.

### progress\_name

A string used by ["execute"](#execute) to assist in creating a progress bar.  Ignored if
["progress\_bar"](#progress_bar) is already specified.

This is the preferred way of customizing the progress bar without having to create one
from scratch.

### cldr

A [CLDR::Number](https://metacpan.org/pod/CLDR::Number) object.  English speakers that use a typical `1,234.56` format would
probably want to leave it at the default.  Otherwise, you should provide your own.

### debug

Boolean.  If turned on, displays timing stats on each chunk, as well as total numbers.

## Common Attributes

### id\_name

The column name used as the iterator in the processing loops.  This should be a primary
key or integer-based (indexed) key, tied to the [resultset](#rs).

Optional.  Used mainly in DBIC processing.  If not specified, it will look up
the first primary key column from ["rs"](#rs) and use that.

This can still be specified for other processing modes to use in progress bars.

### coderef

The coderef that will be called either on each chunk or each row, depending on how
["single\_rows"](#single_rows) is set.  The first input is always the BatchChunker object.  The rest
vary depending on the processing mode:

```perl
$sth + $coderef                  = Query DBI Processing  | $bc->$coderef($executed_sth)
$sth + $coderef + single_rows=>1 = Query DBI Processing  | $bc->$coderef($row_hashref)
$rs  + $coderef                  = DBIC Processing       | $bc->$coderef($chunk_rs)
$rs  + $coderef + single_rows=>1 = DBIC Processing       | $bc->$coderef($result)
       $coderef                  = DIY Processing        | $bc->$coderef($start, $end)
```

The loop does not monitor the return values from the coderef.

Required for all processing modes except ["Active DBI Processing"](#active-dbi-processing).

### chunk\_size

The amount of rows to be processed in each loop.

Default is 1000 rows.  This figure should be sized to keep per-chunk processing time
at around 10 seconds.  If this is too large, rows may lock for too long.  If it's too
small, processing may be unnecessarily slow.

### target\_time

The target runtime (in seconds) that chunk processing should strive to achieve, not
including ["sleep"](#sleep).  If the chunk processing times are too high or too low, this will
dynamically adjust ["chunk\_size"](#chunk_size) to try to match the target.

**Turning this on does not mean you should ignore `chunk_size`!**  If the starting chunk
size is grossly inaccurate to the workload, you could end up with several chunks in the
beginning causing long-lasting locks before the runtime targeting reduces them down to a
reasonable size.

Default is 0, which turns off runtime targeting.

### sleep

The number of seconds to sleep after each chunk.  It uses [Time::HiRes](https://metacpan.org/pod/Time::HiRes)'s version, so
fractional numbers are allowed.

Default is 0, but it is highly recommended to turn this on (say, 5 to 10 seconds) for
really long one-off DB operations, especially if a lot of disk I/O is involved.  Without
this, there's a chance that the slaves will have a hard time keeping up, and/or the
master won't have enough processing power to keep up with standard load.

This will increase the overall processing time of the loop, so try to find a balance
between the two.

### process\_past\_max

Boolean that controls whether to check past the ["max\_id"](#max_id) during the loop.  If the loop
hits the end point, it will run another maximum ID check in the DB, and adjust `max_id`
accordingly.  If it somehow cannot run a DB check (no ["rs"](#rs) or ["max\_sth"](#max_sth) available,
for example), the last chunk will check all the way to `$DB_MAX_ID`.

This is useful if the entire table is expected to be processed, and you don't want to
miss any new rows that come up between ["calculate\_ranges"](#calculate_ranges) and the end of the loop.

Turned off by default.

**NOTE:** If your RDBMS has a problem with a number as high as whatever [max\_integer](https://metacpan.org/pod/Data::Float#max_integer)
reports, you may want to set the `$DB_MAX_ID` global variable in this module to
something lower.

### single\_rows

Boolean that controls whether single rows are passed to the ["coderef"](#coderef) or the chunk's
ResultSets/statement handle is passed.

Since running single-row operations in a DB is painfully slow (compared to bulk
operations), this also controls whether the entire set of coderefs are encapsulated into
a DB transaction.  Transactionalizing the entire chunk brings the speed, and atomicity,
back to what a bulk operation would be.  (Bulk operations are still faster, but you can't
do anything you want in a single DML statement.)

Used only by ["DBIC Processing"](#dbic-processing) and ["Query DBI Processing"](#query-dbi-processing).

### min\_chunk\_percent

The minimum row count, as a percentage of ["chunk\_size"](#chunk_size).  This value is actually
expressed in decimal form, i.e.: between 0 and 1.

This value will be used to determine when to process, skip, or expand a block, based on
a count query.  The default is `0.5` or 50%, which means that it will try to expand the
block to a larger size if the row count is less than 50% of the chunk size.  Zero-sized
blocks will be skipped entirely.

This "chunk resizing" is useful for large regions of the table that have been deleted, or
when the incrementing ID has large gaps in it for other reasons.  Wasting time on
numerical gaps that span millions can slow down the processing considerably, especially
if ["sleep"](#sleep) is enabled.

If this needs to be disabled, set this to 0.  The maximum chunk percentage does not have
a setting and is hard-coded at `100% + min_chunk_percent`.

Used only by ["DBIC Processing"](#dbic-processing) and ["Query DBI Processing"](#query-dbi-processing).  For the latter,
["count\_sth"](#count_sth) is also required to enable chunk resizing.

### min\_id

### max\_id

Used by ["execute"](#execute) to figure out the main start and end points.  Calculated by
["calculate\_ranges"](#calculate_ranges).

Manually setting this is not recommended, as each database is different and the
information may have changed between the DB change development and deployment.  Instead,
use ["calculate\_ranges"](#calculate_ranges) to fill in these values right before running the loop.

## Private Attributes

### \_loop\_state

These variables exist solely for the processing loop.  They should be cleared out after
use.  Most of the complexity is needed for chunk resizing.

- timer

    Timer for debug messages.  Always spans the time between debug messages.

- start

    The real start ID that the loop is currently on.  May continue to exist within iterations
    if chunk resizing is trying to find a valid range.  Otherwise, this value will become
    undef when a chunk is finally processed.

- end

    The real end ID that the loop is currently looking at.  This is always redefined at the
    beginning of the loop.

- prev\_end

    Last "processed" value of `end`.  This also includes skipped blocks.  Used in `start`
    calculations and to determine if the end of the loop has been reached.

- max\_end

    The maximum ending ID.  This will be `$DB_MAX_ID` if ["process\_past\_max"](#process_past_max) is set.

- last\_range

    A hashref of keys used for the bisecting of one block.  Cleared out after a block has
    been processed or skipped.

- last\_timings

    An arrayref of hashrefs, containing data for the previous 5 runs.  This data is used for
    runtime targeting.

- multiplier\_range

    The range (in units of ["chunk\_size"](#chunk_size)) between the start and end IDs.  This starts at 1
    (at the beginning of the loop), but may expand or shrink depending on chunk count checks.
    Resets after block processing.

- multiplier\_step

    Determines how fast multiplier\_range increases, so that chunk resizing happens at an
    accelerated pace.  Speeds or slows depending on what kind of limits the chunk count
    checks are hitting.  Resets after block processing.

- checked\_count

    A check counter to make sure the chunk resizing isn't taking too long.  After ten checks,
    it will give up, assuming the block is safe to process.

- chunk\_size

    The _current_ chunk size, which might be adjusted by runtime targeting.

- chunk\_count

    Records the results of the `COUNT(*)` query for chunk resizing.

- prev\_check

    A short string recording what happened during the last chunk resizing check.  Exists
    purely for debugging purposes.

- prev\_runtime

    The number of seconds the previously processed chunk took to run, not including sleep
    time.

- progress\_bar

    The progress bar being used in the loop.  This may be different than ["progress\_bar"](#progress_bar),
    since it could be auto-generated.

# CONSTRUCTORS

See ["ATTRIBUTES"](#attributes) for information on what can be passed into these constructors.

## new

```perl
my $batch_chunker = DBIx::BatchChunker->new(...);
```

A standard object constructor. If you use this constructor, you will need to
manually call ["calculate\_ranges"](#calculate_ranges) and ["execute"](#execute) to execute the DB changes.

## construct\_and\_execute

```perl
my $batch_chunker = DBIx::BatchChunker->construct_and_execute(...);
```

Constructs a DBIx::BatchChunker object and automatically calls
["calculate\_ranges"](#calculate_ranges) and ["execute"](#execute) on it. Anything passed to this method will be passed
through to the constructor.

Returns the constructed object, post-execution.  This is typically only useful if you want
to inspect the attributes after the process has finished.  Otherwise, it's safe to just
ignore the return and throw away the object immediately.

# METHODS

## calculate\_ranges

```perl
my $batch_chunker = DBIx::BatchChunker->new(
    rsc     => $account_rsc,  # a ResultSetColumn
    ### OR ###
    rs      => $account_rs,   # a ResultSet
    id_name => 'account_id',  # can be looked up if not provided
    ### OR ###
    min_sth => $min_sth,      # a DBI statement handle
    max_sth => $max_sth,      # ditto

    ### Optional but recommended ###
    id_name      => 'account_id',  # will also be added into the progress bar title
    chunk_size   => 20_000,        # default is 1000

    ### Optional ###
    progress_bar => $progress,     # defaults to a 2-count 'Calculating ranges' bar

    # ...other attributes for execute...
);

my $has_data_to_process = $batch_chunker->calculate_ranges;
```

Given a [DBIx::Class::ResultSetColumn](https://metacpan.org/pod/DBIx::Class::ResultSetColumn), [DBIx::Class::ResultSet](https://metacpan.org/pod/DBIx::Class::ResultSet), or [DBI](https://metacpan.org/pod/DBI) statement
handle set, this method calculates the min/max IDs of those objects.  It fills in the
["min\_id"](#min_id) and ["max\_id"](#max_id) attributes, based on the ID data, and then returns 1.

If either of the min/max statements don't return any ID data, this method will return 0.

## execute

```perl
my $batch_chunker = DBIx::BatchChunker->new(
    # ...other attributes for calculate_ranges...

    sth       => $do_sth,       # INSERT/UPDATE/DELETE $sth with BETWEEN placeholders
    ### OR ###
    sth       => $select_sth,   # SELECT $sth with BETWEEN placeholders
    count_sth => $count_sth,    # SELECT COUNT $sth to be used for min_chunk_percent; optional
    coderef   => $coderef,      # called code that does the actual work
    ### OR ###
    rs        => $account_rs,   # base ResultSet, which gets filtered with -between later on
    id_name   => 'account_id',  # can be looked up if not provided
    coderef   => $coderef,      # called code that does the actual work
    ### OR ###
    coderef   => $coderef,      # DIY database work; just pass the $start/$end IDs

    ### Optional but recommended ###
    sleep             => 5,    # number of seconds to sleep each chunk; defaults to 0
    process_past_max  => 1,    # use this if processing the whole table
    single_rows       => 1,    # does $coderef get a single $row or the whole $chunk_rs / $sth
    min_chunk_percent => 0.25, # minimum row count of chunk size percentage; defaults to 0.5 (or 50%)
    target_time       => 15,   # target runtime for dynamic chunk size scaling; default is off

    progress_name => 'Updating Accounts',  # easier than creating your own progress_bar

    ### Optional ###
    progress_bar     => $progress,  # defaults to "Processing $source_name" bar
    debug            => 1,          # displays timing stats on each chunk
);

$batch_chunker->execute if $batch_chunker->calculate_ranges;
```

Applies the configured DB changes in chunks.  Runs through the loop, processing a
statement handle, ResultSet, and/or coderef as it goes.  Each loop iteration processes a
chunk of work, determined by ["chunk\_size"](#chunk_size).

The ["calculate\_ranges"](#calculate_ranges) method should be run first to fill in ["min\_id"](#min_id) and ["max\_id"](#max_id).
If either of these are missing, the function will assume ["calculate\_ranges"](#calculate_ranges) couldn't
find them and warn about it.

More details can be found in the ["Processing Modes"](#processing-modes) and ["ATTRIBUTES"](#attributes) sections.

# PRIVATE METHODS

## \_process\_past\_max\_checker

Checks to make sure the current endpoint is actually the end, by checking the database.
Its return value determines whether the block should be processed or not.

See ["process\_past\_max"](#process_past_max).

## \_chunk\_count\_checker

Checks the chunk count to make sure it's properly sized.  If not, it will try to shrink
or expand the current chunk (in `chunk_size` increments) as necessary.  Its return value
determines whether the block should be processed or not.

See ["min\_chunk\_percent"](#min_chunk_percent).

This is not to be confused with the ["\_runtime\_checker"](#_runtime_checker), which adjusts `chunk_size`
after processing, based on previous run times.

## \_runtime\_checker

Stores the previously processed chunk's runtime, and then adjusts `chunk_size` as
necessary.

See ["target\_time"](#target_time).

## \_increment\_progress

Increments the progress bar.

## \_print\_debug\_status

Prints out a standard debug status line, if debug is enabled.  What it prints is
generally uniform, but it depends on the processing action.  Most of the data is
pulled from ["\_loop\_state"](#_loop_state).

# SEE ALSO

[DBIx::BulkLoader::Mysql](https://metacpan.org/pod/DBIx::BulkLoader::Mysql), [DBIx::Class::BatchUpdate](https://metacpan.org/pod/DBIx::Class::BatchUpdate), [DBIx::BulkUtil](https://metacpan.org/pod/DBIx::BulkUtil)

# AUTHOR

Grant Street Group &lt;developers@grantstreet.com>

# LICENSE AND COPYRIGHT

Copyright 2018 Grant Street Group

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

[http://www.perlfoundation.org/artistic\_license\_2\_0](http://www.perlfoundation.org/artistic_license_2_0)
