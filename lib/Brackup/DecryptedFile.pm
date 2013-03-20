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

package Brackup::DecryptedFile;

use strict;
use warnings;
use Carp qw(croak);
use Brackup::Decrypt;

sub new {
  my ($class, %opts) = @_;
  my $self = bless {}, $class;

  $self->{original_file} = delete $opts{filename};    # filename we're restoring from

  die "File $self->{original_file} does not exist"
        unless $self->{original_file} && -f $self->{original_file};
  croak("Unknown options: " . join(', ', keys %opts)) if %opts;

  # decrypted_file might be undef if no decryption was needed.
  $self->{decrypted_file} = Brackup::Decrypt::decrypt_file_if_needed($self->{original_file});

  return $self;
}

sub name {
    my $self = shift;
    return $self->{decrypted_file} || $self->{original_file};
}

sub DESTROY {
    my $self = shift;
    unlink(grep { $_ } ($self->{decrypted_file}));
}

1;
