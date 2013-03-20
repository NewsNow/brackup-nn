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

package Brackup;
use strict;
use vars qw($VERSION);
$VERSION = '1.10';

use Brackup::Config;
use Brackup::ConfigSection;
use Brackup::File;
use Brackup::Metafile;
use Brackup::PositionedChunk;
use Brackup::StoredChunk;
use Brackup::Backup;
use Brackup::Root;     # aka "source"
use Brackup::Restore;
use Brackup::Target;
use Brackup::BackupStats;

1;

__END__

=head1 NAME

Brackup - Flexible backup tool.  Slices, dices, encrypts, and sprays across the net.

=head1 FURTHER READING

L<Brackup::Manual::Overview>

L<brackup>

L<brackup-restore>

L<brackup-target>


