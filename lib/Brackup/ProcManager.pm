package Brackup::ProcManager;
use strict;
use warnings;
use Brackup::GPGProcess;
use POSIX ":sys_wait_h";

our @HANDLERS;

# A handler is called with the child PID and $?.
# It should return (1, $retvalue) if it recognised the child, (undef, ...) otherwise.

sub add_handler {
    my ($class, $obj, $method) = @_;
    push @HANDLERS, [$obj, $method];
}

# Returns $retvalue (from above) if a child was found; undef otherwise.
sub wait_for_kid {
    my ($class, $blocking) = @_;
    my $flags = $blocking ? 0 : WNOHANG;
    my $pid = waitpid(-1, $flags);

    return undef if $pid <= 0;

    my $retvalue;
    my $flag;

    foreach my $h (@HANDLERS){
        my $method = $h->[1];
        ($flag, $retvalue) = $h->[0]->$method($pid, $?);
        last if $flag;
    }

    die "Unrecognised child '$pid'" unless $flag;
    return $retvalue;
}

1;