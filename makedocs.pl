#!/usr/bin/perl

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

use strict;

my $base = "/home/lj/htdocs/dev/brackup/";
my $pshb = Goats->new;
$pshb->batch_convert([qw(brackup brackup-restore lib)], $base);

package Goats;

use strict;
use base 'Pod::Simple::HTMLBatch';

sub modnames2paths {
    my ($self, $dirs) = @_;

    my @files;
    my @dirs;

    foreach my $path (@{$dirs || []}) {
        if (-f $path) {
            push @files, $path;
        } else {
            push @dirs, $path;
        }
    }

    my $m2p = $self->SUPER::modnames2paths(\@dirs);

    foreach my $file (@files) {
        my ($tail) = $file =~ m!([^/]+)\z!;
        $m2p->{$tail} = $file;
    }

    # these are symlinks in brad's lib
    foreach my $k (keys %$m2p) {
        delete $m2p->{$k} if $k eq "Danga::blib::lib::Danga::Socket" || $k eq "Danga::Socket";
    }

    return $m2p;
}
