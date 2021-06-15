package DBIx::BatchChunker::LoopState;

our $AUTHORITY = 'cpan:GSG';
# ABSTRACT: Loop state object for DBIx::BatchChunker
# VERSION

use Moo;
use MooX::StrictConstructor;

use Types::Standard qw( InstanceOf ArrayRef HashRef Int Str Num Maybe );
use Types::Numbers  qw( UnsignedInt PositiveNum PositiveOrZeroNum FloatSafeNum );
use Type::Utils;

use Math::BigInt upgrade => 'Math::BigFloat';
use Math::BigFloat;
use Time::HiRes qw( time );

# Don't export the above, but don't conflict with StrictConstructor, either
use namespace::clean -except => [qw< new meta >];

=encoding utf8

=head1 SYNOPSIS

    sub chunk_method {
        my ($bc, $rs) = @_;

        my $loop_state = $bc->loop_state;
        # introspect stuff
    }

=head1 DESCRIPTION

This is the loop state object used during BatchChunker runs.  It only exists within the
BatchChunker execution loop, and would generally only be accessible through the coderef
or method referenced within that loop.

This is a quasi-private object and its API may be subject to change, but the module is
in a pretty stable state at this point.  While permissions are available to write to
the attributes, it is highly recommended to not do so unless you know exactly what you
doing.  These are mostly available for introspection of loop progress.

=head1 ATTRIBUTES

=head2 batch_chunker

Reference back to the parent L<DBIx::BatchChunker> object.

=cut

has batch_chunker => (
    is       => 'ro',
    isa      => InstanceOf['DBIx::BatchChunker'],
    required => 1,
    weak_ref => 1,
);

=head2 progress_bar

The progress bar being used in the loop.  This may be different than
L<DBIx::BatchChunker/progress_bar>, since it could be auto-generated.

If you're trying to access the progress bar for debug or display purposes, it's best to
use this attribute:

    my $progress_bar = $bc->loop_state->progress_bar;
    $progress_bar->message('Found something here');

=cut

has progress_bar => (
    is       => 'rw',
    isa      => InstanceOf['Term::ProgressBar'],
    required => 1,
);

=head2 timer

Timer for debug messages.  Always spans the time between debug messages.

=cut

has timer => (
    is       => 'rw',
    isa      => PositiveNum,
    default  => sub { time() },
);

sub _mark_timer { shift->timer(time); }

=head2 start

The real start ID that the loop is currently on.  May continue to exist within iterations
if chunk resizing is trying to find a valid range.  Otherwise, this value will become
undef when a chunk is finally processed.

=cut

has start => (
    is       => 'rw',
    isa      => Maybe[UnsignedInt],
    lazy     => 1,
    default  => sub { shift->batch_chunker->min_id },
);

=head2 end

The real end ID that the loop is currently looking at.  This is always redefined at the
beginning of the loop.

=cut

has end => (
    is       => 'rw',
    isa      => UnsignedInt,
    lazy     => 1,
    default  => sub {
        my $self = shift;
        $self->start + $self->batch_chunker->chunk_size - 1;
    },
);

=head2 prev_end

Last "processed" value of L</end>.  This also includes skipped blocks.  Used in L</start>
calculations and to determine if the end of the loop has been reached.

=cut

has prev_end => (
    is       => 'rw',
    isa      => UnsignedInt,
    lazy     => 1,
    default  => sub { shift->start - 1 },
);

=head2 last_range

A hashref of min/max values used for the bisecting of one block, measured in chunk
multipliers.  Cleared out after a block has been processed or skipped.

=cut

has last_range => (
    is       => 'rw',
    isa      => HashRef,
    default  => sub { {} },
);

=head2 last_timings

An arrayref of hashrefs, containing data for the previous 5 runs.  This data is used for
runtime targeting.

=cut

has last_timings => (
    is       => 'rw',
    isa      => ArrayRef,
    default  => sub { [] },
);

sub _reset_last_timings { shift->last_timings([]) }

=head2 multiplier_range

The range (in units of L</chunk_size>) between the start and end IDs.  This starts at 1
(at the beginning of the loop), but may expand or shrink depending on chunk count checks.
Resets after block processing.

=cut

has multiplier_range => (
    is       => 'rw',
    isa      => FloatSafeNum,
    lazy     => 1,
    default  => sub {
        shift->batch_chunker->_use_bignums ? Math::BigFloat->new(0) : 0;
    },
);

=head2 multiplier_step

Determines how fast L</multiplier_range> increases, so that chunk resizing happens at an
accelerated pace.  Speeds or slows depending on what kind of limits the chunk count
checks are hitting.  Resets after block processing.

=cut

has multiplier_step => (
    is       => 'rw',
    isa      => FloatSafeNum,
    lazy     => 1,
    default  => sub {
        shift->batch_chunker->_use_bignums ? Math::BigFloat->new(1) : 1;
    },
);

=head2 checked_count

A check counter to make sure the chunk resizing isn't taking too long.  After ten checks,
it will give up, assuming the block is safe to process.

=cut

has checked_count => (
    is       => 'rw',
    isa      => Int,
    default  => 0,
);

=head2 chunk_size

The I<current> chunk size, which might be adjusted by runtime targeting.

=cut

has chunk_size => (
    is       => 'rw',
    isa      => UnsignedInt,
    lazy     => 1,
    default  => sub { shift->batch_chunker->chunk_size },
);

=head2 chunk_count

Records the results of the C<COUNT(*)> query for chunk resizing.

=cut

has chunk_count => (
    is       => 'rw',
    isa      => Maybe[UnsignedInt],
    default  => undef,
);

=head2 prev_check

A short string recording what happened during the last chunk resizing check.  Exists
purely for debugging purposes.

=cut

has prev_check => (
    is       => 'rw',
    isa      => Str,
    default  => '',
);

=head2 prev_runtime

The number of seconds the previously processed chunk took to run, not including sleep
time.

=cut

has prev_runtime => (
    is       => 'rw',
    isa      => Maybe[PositiveOrZeroNum],
    default  => undef,
);

sub _reset_chunk_state {
    my $ls = shift;
    $ls->start   (undef);
    $ls->prev_end($ls->end);
    $ls->_mark_timer;

    $ls->last_range      ({});
    $ls->multiplier_range(0);
    $ls->multiplier_step (1);
    $ls->checked_count   (0);

    if ($ls->batch_chunker->_use_bignums) {
        $ls->multiplier_range( Math::BigFloat->new(0) );
        $ls->multiplier_step ( Math::BigFloat->new(1) );
    }
}

1;
