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

package Brackup::GPGProcManager;
use strict;
use warnings;
use Brackup::GPGProcess;
use Brackup::ProcManager;
use Brackup::Util qw(tempfile_obj);
use POSIX ":sys_wait_h";

sub new {
    my ($class, $iter, $target) = @_;

    my $me = bless {
        chunkiter => $iter,
        procs     => {},  # "addr(pchunk)" => GPGProcess
        target    => $target,
        procs_running => {}, # pid -> GPGProcess
        uncollected_bytes => 0,
        uncollected_chunks => 0,
    }, $class;

    Brackup::ProcManager->set_maximum('gpg', $target->{gpg_daemons});

    return $me;
}

sub gen_process_for {
    my ($self, $pchunk) = @_;

    return Brackup::ProcManager->start_child('gpg', $self, 'child_handler', {
        'pchunk' => $pchunk,
        'destfh' => tempfile_obj()
    });
}

sub child_handler {
    my ($self, $flag, $data) = @_;

    if($flag eq 'inchild'){

        my $pchunk = $data->{data}->{pchunk};
        return Brackup::GPGProcess->encrypt($pchunk, $data->{data}->{destfh}); # exit code of process

    }
    elsif($flag eq 'inparent'){

        my $pid = $data->{pid};
        my $pchunk = $data->{data}->{pchunk};
        my $proc = Brackup::GPGProcess->new($pid, $data->{data}->{destfh});
        $self->{procs_running}{$pid} = $proc;
        $self->{procs}{$pchunk} = $proc;
        $self->{uncollected_chunks}++;
        return $proc; # returned by start_child

    }
    elsif($flag eq 'childexit'){

        my $pid = $data->{pid};
        my $proc = $self->{procs_running}{$pid};
        delete $self->{procs_running}{$pid} or die 'ASSERT';
        $proc->note_stopped;
        $self->{uncollected_bytes} += $proc->size_on_disk;
        return $pid; # returned by wait_for_child

    }
}

sub enc_chunkref_of {
    my ($self, $pchunk) = @_;

    my $proc = $self->{procs}{$pchunk};
    unless ($proc) {
        # catch iterator up to the point that was
        # requested, or blow up.
        my $found = 0;
        my $iters = 0;
        while (my $ich = $self->{chunkiter}->next) {
            if ($ich == $pchunk) {
                $found = 1;
                last;
            }
            $iters++;
            warn "iters = $iters\n";
        }
        die "Not found" unless $found;
        $proc = $self->gen_process_for($pchunk);
    }

    while ($proc->running) { # wait until this particular process exits
        Brackup::ProcManager->wait_for_child(1);
    }

    $self->_proc_summary_dump;
    my ($cref, $enc_length) = $self->get_proc_chunkref($proc);
    $self->_proc_summary_dump;
    $self->start_some_processes;

    return ($cref, $enc_length);
}

sub start_some_processes {
    my $self = shift;

    Brackup::ProcManager->wait_for_extra_children('gpg');

    my $pchunk;
    while ($self->num_running_procs < $self->{target}->{gpg_daemons} &&
           $self->uncollected_chunks < 20 &&
           $self->num_uncollected_bytes < 128 * 1024 * 1024 &&
           ($pchunk = $self->next_chunk_to_encrypt)) {
        $self->_proc_summary_dump;
        $self->gen_process_for($pchunk);
        $self->_proc_summary_dump;
    }
}

sub _proc_summary_dump {
    my $self = shift;
    return unless $ENV{GPG_DEBUG};

    printf STDERR "num_running=%d, num_outstanding_bytes=%d uncollected_chunks=%d\n",
    $self->num_running_procs,  $self->num_uncollected_bytes, $self->uncollected_chunks;
}

sub next_chunk_to_encrypt {
    my $self = shift;
    while (my $ev = $self->{chunkiter}->next) {
        next if $ev->isa("Brackup::File");
        my $pchunk = $ev;

        # WARNING The checks here are coupled to the ones in Backup::backup

        next if $self->{target}->stored_chunk_from_inventory($pchunk);
        next if $self->{target}->is_pchunk_being_stored($pchunk);
        return $pchunk;
    }
    return undef;
}

sub get_proc_chunkref {
    my ($self, $proc) = @_;
    my $cref = $proc->chunkref;
    delete $self->{procs}{$proc};
    $self->{uncollected_bytes} -= $proc->size_on_disk;
    $self->{uncollected_chunks}--;
    return ($cref, $proc->size_on_disk);
}

sub num_uncollected_bytes { $_[0]{uncollected_bytes} }

sub uncollected_chunks { $_[0]{uncollected_chunks} }

sub num_running_procs {
    my $self = shift;
    return scalar keys %{$self->{procs_running}};
}

1;

