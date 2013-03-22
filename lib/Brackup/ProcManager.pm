# LICENCE INFORMATION
#
# This file is part of brackup-nn, a backup tool based on Brackup.
#
# Brackup is authored by Brad Fitzpatrick <brad@danga.com> (and others)
# and is copyright (c) Six Apart, Ltd, with portions copyright (c) Gavin Carr
# <gavin@openfusion.com.au> (see code for details).  Brackup is licensed for
# use, modification and/or distribution under the same terms as Perl itself.
#
# brackup-nn was forked from Brackup on 18 March 2013 and changed on and since
# this date by NewsNow Publishing Limited to effect bug fixes, reliability
# stability and/or performance improvements, and/or feature enhancements;
# and such changes are copyright (c) 2013 NewsNow Publishing Limited.  You may
# use, modify, and/or redistribute brackup-nn under the same terms as Perl itself.
#
# This file is a new addition to brackup-nn.
#

package Brackup::ProcManager;
use strict;
use warnings;
use IO::File;
use POSIX ":sys_wait_h";
use POSIX qw(_exit);

our %CHILDREN;
our %CHILD_GROUP_COUNT;
our %CHILD_GROUP_MAX;

sub set_maximum {
    my $class = shift;
    my $group = shift;
    my $max = shift;
    $CHILD_GROUP_MAX{$group} = $max;
}

# $obj->$method($flag, $DATA) is called with:
#   $flag       where                                          value returned by $method
#   -----       -----                                          -------------------------
# - 'inchild'   in the child process                           is the return code of the process
# - 'inparent'  in the parent process after fork               is what start_child returns
# - 'childexit' in the parent process after the child exited   is what wait_for_child returns
#
# $DATA is a hashref with the following keys:
# - data => $data passed to start_child
# - pid
# - fh (only available at inparent)
# - ret => $? (only available at childexit)
# - retcode => ($? >> 8) & 255

sub start_child {
    my $class = shift;
    my $group = shift; # String to set the maximum
    my $obj = shift; # Object or class
    my $method = shift; # string
    my $data = shift; # string

    # Wait until number of children for this group falls below maximum
    $class->wait_for_extra_children($group);

    # TODO: Die if cannot fork

    my $fh = IO::File->new();
    if(my $pid = open($fh, '-|')) {
        # in parent

        # warn "New child in group '$group' PID '$pid' # '$CHILD_GROUP_COUNT{$group}'\n";

        $CHILDREN{$pid} = {
            'pid' => $pid,
            'fh' => $fh,
            'data' => $data,
            'group' => $group,
            'obj' => $obj,
            'method' => $method
        };
        $CHILD_GROUP_COUNT{$group}++;
        return $obj->$method('inparent', $CHILDREN{$pid});
    }

   # in child

   # See http://perldoc.perl.org/perlfork.html
   # On some operating systems, notably Solaris and Unixware, calling exit()
   # from a child process will flush and close open filehandles in the parent,
   # thereby corrupting the filehandles. On these systems, calling _exit() is
   # suggested instead.

    my $r;
    unless(eval {
        $r = $obj->$method('inchild', {'data'=>$data, 'pid'=>$$});
        1;
    }){
        warn "Child in group '$group' died '$@'\n";
        $r = -1;
    }
    File::Temp::cleanup(); # only removes temp resources created by the child process
    _exit($r);

}

# Returns what the handler method returns unless $ret_bool is true,
# in which case always returns true if a child was found.
# Returns undef if no child was found.
sub wait_for_child {
    my ($class, $blocking, $ret_bool) = @_;
    my $flags = $blocking ? 0 : WNOHANG;
    my $pid = waitpid(-1, $flags);

    return undef if $pid <= 0;

    die "Unrecognised child '$pid'" unless $CHILDREN{$pid};

    # warn "Child exited in group '$CHILDREN{$pid}->{group}' PID '$pid' # '$CHILD_GROUP_COUNT{$CHILDREN{$pid}->{group}}'\n";

    $CHILDREN{$pid}->{ret} = $?;
    $CHILDREN{$pid}->{retcode} = ($CHILDREN{$pid}->{ret} >> 8) & 255;
    $CHILD_GROUP_COUNT{ $CHILDREN{$pid}->{group} }--;
    my $method = $CHILDREN{$pid}->{method};
    my $r = $CHILDREN{$pid}->{obj}->$method('childexit', $CHILDREN{$pid});
    close $CHILDREN{$pid}->{fh};
    delete $CHILDREN{$pid};
    return 1 if $ret_bool;
    return $r;
}

# Wait for children until their number falls below the maximum
sub wait_for_extra_children {
    my $class = shift;
    my $group = shift;

    if($CHILD_GROUP_MAX{$group}){
        while(($CHILD_GROUP_COUNT{$group} || 0) >= $CHILD_GROUP_MAX{$group}){
            $class->wait_for_child(1);
        }
    }
}

# Wait for all children in the group
sub wait_for_all_children {
    my $class = shift;
    my $group = shift;

    while(($CHILD_GROUP_COUNT{$group} || 0) > 0){
        $class->wait_for_child(1);
    }
}

sub collect_zombies {
    my $class = shift;

    while( $class->wait_for_child(0, 1) ){ 1; }
}

sub assert_all_reaped {
    my $class = shift;

    foreach my $group (keys %CHILD_GROUP_COUNT){
        if( $CHILD_GROUP_COUNT{$group} ){
            die "ASSERT: Some children in group '$group' haven't been reaped\n";
        }
    }
}

# Calls &$callback( $child_hashref ) for each child in $group.
# Return true from the callback to continue the loop.
# Returns true if the callback has returned false.
sub for_each_child {
    my $class = shift;
    my $group = shift;
    my $callback = shift;

    my $stopped;
    foreach my $o (values %CHILDREN){
        next unless $o->{group} eq $group;
        unless(&$callback($o)){
            $stopped = 1;
            last;
        }
    }

    return $stopped;
}

1;