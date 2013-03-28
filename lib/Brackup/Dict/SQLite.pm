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

package Brackup::Dict::SQLite;
use strict;
use warnings;
use DBI;
use DBD::SQLite;

sub new {
    my ($class, %opts) = @_;
    my $self = bless {
        table => $opts{table},
        file  => $opts{file},
        data  => {},
    }, $class;

    unless( $opts{allow_new} || -e $opts{file} ){
        die "[NO_DB] DB file not found - need to create new one?";
    }

    my $dbh = $self->{dbh} = DBI->connect("dbi:SQLite:dbname=$opts{file}","","", { RaiseError => 1, PrintError => 0 }) or
        die "Failed to connect to SQLite filesystem digest cache database at $opts{file}: " . DBI->errstr;

    eval {
        $dbh->do("CREATE TABLE $opts{table} (key TEXT PRIMARY KEY, value TEXT)");
    };
    die "Error: $@" if $@ && $@ !~ /table \w+ already exists/;

    $dbh->do("PRAGMA synchronous = OFF");
    # $dbh->do("BEGIN TRANSACTION");

    return $self;
}

# DESTROY { my ($self) = @_; $self->{dbh}->do("END TRANSACTION") if $self->{dbh}; }

sub _reset {
    my $self = shift;
    $self->{data} = {};
    $self->{keys} = [];
    $self->{_loaded_keys} = 0;
}

sub _load_all
{
    my $self = shift;
    unless ($self->{_loaded_all}++) {
        # SQLite sucks at doing anything quickly (likes hundred thousand
        # selects back-to-back), so we just suck the whole damn thing into
        # a perl hash.  cute, huh?  then it doesn't have to
        # open/read/seek/seek/seek/read/close for each select later.
        $self->_reset;
        my $sth = $self->{dbh}->prepare("SELECT key, value FROM $self->{table}");
        $sth->execute;
        while (my ($k, $v) = $sth->fetchrow_array) {
            $self->{data}{$k} = $v;
        }
    }
}

sub get {
    my ($self, $key) = @_;
    $self->_load_all unless $self->{_loaded_all};
    return $self->{data}{$key};
}

sub set {
    my ($self, $key, $val) = @_;
    $self->{dbh}->do("REPLACE INTO $self->{table} VALUES (?,?)", undef, $key, $val);
    $self->{data}{$key} = $val;
    return 1;
}

# Iterator interface, returning ($key, $value), and () on eod
sub each {
    my $self = shift;
    $self->_load_all unless $self->{_loaded_all};
    $self->{keys} = [ keys %{$self->{data}} ] unless $self->{_loaded_keys}++;
    if (! @{$self->{keys}}) {
        $self->{_loaded_keys} = 0;
        return wantarray ? () : undef;
    }
    my $next = shift @{$self->{keys}};
    return wantarray ? ($next, $self->{data}{$next}) : $next;
}

sub delete {
    my ($self, $key) = @_;
    $self->{dbh}->do("DELETE FROM $self->{table} WHERE key = ?", undef, $key);
    delete $self->{data}{$key};
    return 1;
}

sub count {
    my $self = shift;
    $self->_load_all unless $self->{_loaded_all};
    return scalar keys %{$self->{data}};
}

sub backing_file {
    my $self = shift;
    return $self->{file};
}

sub wipe {
    die "not implemented";
}

1;

__END__

=head1 NAME

Brackup::Dict::SQLite - key-value dictionary implementation, using a
SQLite database for storage

=head1 DESCRIPTION

Brackup::Dict::SQLite implements a simple key-value dictionary using
a SQLite database (in a single file) for storage. It provides the
default storage backend for both the L<Brackup::DigestCache> digest
cache and the L<Brackup::InventoryDatabase> inventory database (as
separate databases). The database schema is created automatically as
needed - no database maintenance is required.

Brackup::Dict::SQLite is optimised for speed and loads the entire
database into memory at startup. If you wish to trade-off some
performance for a more conservative memory footprint, you should
consider using L<Brackup::Dict::SQLite2> instead.

See L<Brackup::DigestCache> and L<Brackup::InventoryDatabase> for
how to manually specify the dictionary class to use.

=head1 DETAILS

=head2 File location

The database file location is a parameter defined by the using class,
so see L<Brackup::DigestCache> and L<Brackup::InventoryDatabase> for
their respective database locations.

=head2 SQLite Schema

This is defined automatically, but if you want to look around in it,
the schema is:

  CREATE TABLE <TABLE> (
       key TEXT PRIMARY KEY,
       value TEXT
  )

=head1 SEE ALSO

L<brackup>

L<Brackup>

L<Brackup::Dict::SQLite2>

=cut
