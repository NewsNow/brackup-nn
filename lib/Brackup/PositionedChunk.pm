# LICENCE INFORMATION
#
# This file is part of brackup-nn, a backup tool based on Brackup.
#
# Brackup is authored by Brad Fitzpatrick <brad@danga.com> (and others)
# and is copyright (c) Six Apart, Ltd, with portions copyright (c) Gavin Carr
# <gavin@openfusion.com.au> (see code for details).  Brackup is licensed for
# use, modification and/or distribution under the same terms as Perl itself.
#
# This file was forked from Brackup on 18 March 2013 and changed on and since
# this date by NewsNow Publishing Limited to effect bug fixes, reliability
# stability and/or performance improvements, and/or feature enhancements;
# and such changes are copyright (c) 2013 NewsNow Publishing Limited.  You may
# use, modify, and/or redistribute brackup-nn under the same terms as Perl itself.
#

package Brackup::PositionedChunk;

use strict;
use warnings;
use Carp qw(croak);
use Brackup::Util qw(io_sha1);
use IO::File;
use IO::InnerFile;
use Fcntl qw(SEEK_SET);

use fields (
            'file',     # the Brackup::File object
            'offset',   # offset within said file
            'length',   # length of data
            '_raw_digest',
            '_raw_chunkref',
            );

sub new {
    my ($class, %opts) = @_;
    my $self = ref $class ? $class : fields::new($class);

    $self->{file}   = delete $opts{'file'};    # Brackup::File object
    $self->{offset} = delete $opts{'offset'};
    $self->{length} = delete $opts{'length'};

    croak("Unknown options: " . join(', ', keys %opts)) if %opts;
    croak("offset not numeric") unless $self->{offset} =~ /^\d+$/;
    croak("length not numeric") unless $self->{length} =~ /^\d+$/;
    return $self;
}

sub as_string {
    my $self = shift;
    return $self->{file}->as_string . "{off=$self->{offset},len=$self->{length}}";
}

# the original length, pre-encryption
sub length {
    my $self = shift;
    return $self->{length};
}

sub offset {
    my $self = shift;
    return $self->{offset};
}

sub file {
    my $self = shift;
    return $self->{file};
}

sub root {
    my $self = shift;
    return $self->file->root;
}

sub has_raw_digest {
    my $self = shift;
    return $self->{_raw_digest};
}

sub raw_digest {
    my $self = shift;
    return $self->{_raw_digest} ||= $self->_calc_raw_digest;
}

sub _calc_raw_digest {
    my $self = shift;

    my $n_chunks = $self->{file}->chunks
        or die "zero chunks?";
    if ($n_chunks == 1) {
        # don't calculate this chunk's digest.. it's the same as our
        # file's digest, since this chunk spans the entire file.
        die "ASSERT" unless $self->length == $self->{file}->size;
        return $self->{file}->full_digest;
    }

    my $cache = $self->root->digest_cache;
    my $key   = $self->cachekey;
    my $dig;

    if ($dig = $cache->get($key)) {
        return $self->{_raw_digest} = $dig;
    }

    $dig = "sha1:" . io_sha1($self->raw_chunkref);

    $cache->set($key => $dig);

    return $self->{_raw_digest} = $dig;
}

sub raw_chunkref {
    my $self = shift;
    if ($self->{_raw_chunkref}) {
      $self->{_raw_chunkref}->seek(0, SEEK_SET);
      return $self->{_raw_chunkref};
    }

    my $fullpath = $self->{file}->fullpath;

    my $fh = IO::File->new($fullpath, 'r') or die "[SKIP_FILE] Failed to open $fullpath: $!";
    binmode($fh);

    my $ifh = IO::InnerFile->new($fh, $self->{offset}, $self->{length})
        or die "[SKIP_FILE] Failed to create inner file handle for $fullpath: $!\n";
    return $self->{_raw_chunkref} = $ifh;
}

# useful string for targets to key on.  of one of the forms:
# {INVSYNTAX} (search for this label to see where else this syntax is used)
#    "<digest>;to=<enc_to>"
#    "<digest>;raw"
#    "<digest>;gz"   (future)
sub inventory_key {
    my $self = shift;
    my $separator = shift || ';';

    my $key = $self->raw_digest;
    if (my @rcpts = $self->root->gpg_rcpts) {
        $key .= $separator . "to=" . join('_', @rcpts);
    } else {
        $key .= $separator . "raw";
    }
    return $key;
}

sub forget_chunkref {
    my $self = shift;
    delete $self->{_raw_chunkref};
}

sub cachekey {
    my $self = shift;
    return $self->{file}->cachekey . ";o=$self->{offset};l=$self->{length}";
}

sub is_entire_file {
    my $self = shift;
    return $self->{file}->chunks == 1;
}

1;
