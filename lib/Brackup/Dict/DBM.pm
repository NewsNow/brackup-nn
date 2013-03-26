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

package Brackup::Dict::DBM;
use strict;
use warnings;
BEGIN { @AnyDBM_File::ISA = qw(DB_File GDBM_File NDBM_File SDBM_File) }
use AnyDBM_File;
use Fcntl qw(O_CREAT O_RDWR);

sub new {
    my ($class, %opts) = @_;
    my $self = bless {
        file  => $opts{file},
        data  => {},
    }, $class;

    unless( $opts{create_new} || -e $opts{file} ){
        die "[NO_DB] DB file not found - need to create new one?";
    }

    my %dbm;
    tie %dbm, 'AnyDBM_File', $self->{file}, O_CREAT | O_RDWR, 0644 or
        die "Failed to bind to DBM digest cache at $self->{file}: $!";
    $self->{dbm} = \%dbm;

    return $self;
}

sub get {
    my ($self, $key) = @_;
    return $self->{dbm}->{$key};
}

sub set {
    my ($self, $key, $val) = @_;
    $self->{dbm}->{$key} = $val;
    return 1;
}

# Iterator interface, returning ($key, $value), and () on eod
sub each {
    my $self = shift;
    $self->{keys} = [ keys %{$self->{dbm}} ] unless $self->{_loaded_keys}++;
    if (! @{$self->{keys}}) {
        $self->{_loaded_keys} = 0;
        return wantarray ? () : undef;
    }
    my $next = shift @{$self->{keys}};
    return wantarray ? ($next, $self->{dbm}->{$next}) : $next;
}

sub delete {
    my ($self, $key) = @_;
    delete $self->{dbm}->{$key};
    return 1;
}

sub count {
    my $self = shift;
    return scalar keys %{$self->{dbm}};
}

sub backing_file {
    my $self = shift;
    return $self->{file};
}

1;

