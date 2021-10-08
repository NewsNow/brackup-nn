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

package Brackup::Chunker::Default;
use strict;

sub chunks {
    my ($class, $file) = @_;
    my @chunk_list;

    my $root       = $file->root;
    my $chunk_size = $root->chunk_size;
    my $size       = $file->size;

    my $offset = 0;
    my $count = 0;
    while ($offset < $size) {
        my $len = _min($chunk_size, $size - $offset);
        my $chunk = Brackup::PositionedChunk->new(
                                                  file   => $file,
                                                  offset => $offset,
                                                  length => $len,
                                                  count  => $count
                                                  );
        push @chunk_list, $chunk;
        $offset += $len;
        $count++;
    }
    return @chunk_list;
}

sub _min {
    return (sort { $a <=> $b } @_)[0];
}

1;
