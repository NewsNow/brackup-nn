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

package Brackup::Target::Filebased;
use strict;
use warnings;
use base 'Brackup::Target';

# version >= 1.06: 01/23/0123456789abcdef...xxx.chunk
# 256 * 256 directories, then files.  would need 2 billion
# files before leaves have 32k+ files, but at that point
# users are probably using better filesystems if they
# have 2+ billion inodes.
sub chunkpath {
    my ($self, $dig) = @_;
    my @parts;
    my $fulldig = $dig;

    $dig =~ s/^\w+://; # remove the "hashtype:" from beginning
    $fulldig =~ s/:/./g if $self->nocolons; # Convert colons to dots if we've been asked to

    while (length $dig && @parts < 2) {
        $dig =~ s/^([0-9a-f]{2})// or die "Can't get 2 hex digits of $fulldig";
        push @parts, $1;
    }

    return join("/", @parts) . "/$fulldig.chunk";
}

sub metapath {
    my ($self, $name) = @_;

    $name ||= '';

    return "backups/$name";
}

1;
