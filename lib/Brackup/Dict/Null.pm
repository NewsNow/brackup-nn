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

package Brackup::Dict::Null;

sub new { bless {}, shift }
sub get {}
sub set {}
sub each {}
sub delete {}
sub count { 0 }
sub backing_file {}

1;

__END__

=head1 NAME

Brackup::Dict::Null - noop key-value dictionary implementation, 
discarding everything it receives

=head1 DESCRIPTION

Brackup::Dict::Null is a noop implementation of the Brackup::Dict
inteface - it just discards all data it receives, and returns undef
to all queries. 

Intended for TESTING ONLY.

Ignores all instantiation parameters, and doesn't use any files.

=head1 SEE ALSO

L<brackup>

L<Brackup>

L<Brackup::Dict::SQLite>

=cut
