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

package Brackup::TargetBackupStatInfo;

use strict;
use warnings;
use Carp qw(croak);
use POSIX qw(strftime);

sub new {
    my ($class, $target, $fn, %opts) = @_;
    my $self = {
        target => $target,
        filename => $fn,
        time => delete $opts{time},
        size => delete $opts{size},
    };
    croak "unknown options: " . join(", ", keys %opts) if %opts;

    return bless $self, $class;
}

sub target {
    return $_[0]->{target};
}

sub filename {
    return $_[0]->{filename};
}

sub time {
    return $_[0]->{time};
}

sub localtime {
    return strftime("%a %d %b %Y %T", localtime( $_[0]->{time} ));
}

sub size {
    return $_[0]->{size};
}


1;

