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

package Brackup::Metafile;
use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class) = @_;
    return bless {}, $class;
}

sub open {
    my ($class, $file) = @_;
    unless (-e $file) {
        die "Unable to open metafile $file\n";
    }
    my $self = __PACKAGE__->new;
    $self->{filename} = $file;
    if (eval { require IO::Uncompress::AnyUncompress }) {
        $self->{fh} = IO::Uncompress::AnyUncompress->new($file)
            or die "Failed to open file $file: $IO::Uncompress::AnyUncompress::AnyUncompressError";
    }
    else {
        open $self->{fh}, "<", $file;
    }
    
    $self->{linenum} = 0;
    $self->{data} = [];
    $self->{buffer} = '';
    return $self;
}

sub preload {
    my $self = shift;
    
    my $data;
    my $bytes = read($self->{fh}, $data, 1048576, 0);

    $self->{'data'} = [ split("\n", $self->{'buffer'} . $data, -1) ];
    
    unless($bytes) {
        $self->{buffer} = '';
        return undef;
    }
    
    $self->{'buffer'} = pop(@{$self->{'data'}});
    
    return $bytes;
}

sub line {
    my $self = shift;
    
    while( !scalar(@{$self->{'data'}}) && $self->preload() ) {
        ;
    }
    
    return shift @{$self->{'data'}};
}

sub readline {
    my $self = shift;
    my $ret;
    my $line;
    while (1) {
        # Repeat sub line here for speed.
        while( !scalar(@{$self->{'data'}}) && $self->preload() ) {
            ;
        }
    
        return undef unless defined( $line = shift @{$self->{'data'}} );
         
        $self->{linenum}++;
         
        if ($line eq "") {
            return $ret || {};
        }

        if (substr($line,0,1) eq ' ' && $line =~ /^\s+(.+)/) {
            die "Can't continue line without start" unless $self->{last};
            ${ $self->{last} } .= " $1";
            next;
        }
         
        if ($line =~ /^([\w\-]+):\s*(.+)/) {
            $ret->{$1} = $2;
            $self->{last} = \$ret->{$1};
            next;
        }

        $line =~ s/[^[:print:]]/?/g;
        die "Unexpected line in metafile $self->{filename}, line $self->{linenum}: $line";
    }

    return undef;
}

1;
