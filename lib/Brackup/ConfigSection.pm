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

package Brackup::ConfigSection;
use strict;
use warnings;
use File::Spec;

sub new {
    my ($class, $name) = @_;
    return bless {
        _name      => $name,
        _accessed  => {},  # key => 1
    }, $class;
}

sub name {
    my $self = shift;
    return $self->{_name};
}

sub add {
    my ($self, $key, $val) = @_;
    push @{ $self->{$key} ||= [] }, $val;
}

sub unused_config {
    my $self = shift;
    return sort grep { $_ ne "_name" && $_ ne "_accessed" && ! $self->{_accessed}{$_} } keys %$self;
}

sub path_value {
    my ($self, $key) = @_;
    my $val = $self->value($key) || "";
    die "Path '$key' of '$val' isn't a valid absolute directory path\n"
        unless $val && -d $val && File::Spec->file_name_is_absolute($val);
    return $val;
}

sub file_value_or_empty {
    my ($self, $key) = @_;
    return $self->value($key) unless $self->value($key);
    my $val = $self->value($key) || "";
    die "Path '$key' of '$val' isn't a valid absolute file path\n"
        unless $val && -f $val && File::Spec->file_name_is_absolute($val);
    return $val;
}

sub byte_value {
    my ($self, $key) = @_;
    my $val = $self->value($key);
    return 0                unless $val;
    return $1               if $val =~ /^(\d+)b?$/i;
    return $1 * 1024        if $val =~ /^(\d+)kb?$/i;
    return $1 * 1024 * 1024 if $val =~ /^(\d+)mb?$/i;
    return $1 * 1024 * 1024 * 1024 if $val =~ /^(\d+)gb?$/i;
    die "Unrecognized size format for $key: '$val'\n";
}

sub bool_value {
    my ($self, $key) = @_;
    my $val = $self->value($key);
    return 0 if ! $val;
    return 1 if $val =~ /^(1|true|yes|on)$/i;
    return 0 if $val =~ /^(0|false|no|off)$/i;
    die "Unrecognized boolean value for $key: '$val'\n";
}

sub value {
    my ($self, $key) = @_;
    $self->{_accessed}{$key} = 1;
    my $vals = $self->{$key};
    return undef unless $vals;
    die "Configuration section '$self->{_name}' has multiple values of key '$key' where only one is expected.\n"
        if @$vals > 1;
    return $vals->[0];
}

sub values {
    my ($self, $key) = @_;
    $self->{_accessed}{$key} = 1;
    my $vals = $self->{$key};
    return () unless $vals;
    return @$vals;
}

sub keys {
    my $self = shift;
    return grep !/^_/, keys %$self;
}

1;
