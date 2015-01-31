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

package Brackup::Restore;
use strict;
use warnings;
use Carp qw(croak);
use Digest::SHA1;
use POSIX qw(mkfifo);
use Unix::Mknod;
use Fcntl qw(O_RDONLY O_CREAT O_WRONLY O_TRUNC S_IFCHR S_IFBLK);
use String::Escape qw(unprintable);
use File::stat;
use Try::Tiny;
use Brackup::DecryptedFile;
use Brackup::Decrypt;
use Brackup::ProcManager;

sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;

    $self->{to}       = delete $opts{to};      # directory we're restoring to
    $self->{prefix}   = delete $opts{prefix};  # directory/file filename prefix, or "" for all
    $self->{filename} = delete $opts{file};    # filename we're restoring from
    $self->{config}   = delete $opts{config};  # brackup config (if available)
    $self->{onerror}  = delete $opts{onerror}  || 'abort';
    $self->{conflict} = delete $opts{conflict} || 'abort';
    $self->{verbose}  = delete $opts{verbose};
    $self->{daemons}  = delete $opts{daemons}; # number of processes used to restore files in parallel

    $self->{numeric_ids} = delete $opts{numeric_ids};
    $self->{_local_uid_map} = {};  # remote/metafile uid -> local uid
    $self->{_local_gid_map} = {};  # remote/metafile gid -> local gid

    $self->{no_lchown} = delete $opts{no_lchown};
    
    $self->{prefix} =~ s/\/$// if $self->{prefix};

    $self->{_stats_to_run} = [];  # stack (push/pop) of subrefs to reset stat info on

    die "Destination directory doesn't exist" unless $self->{to} && -d $self->{to};
    croak("Unknown options: " . join(', ', keys %opts)) if %opts;
    
    if (! defined $self->{_lchown}) {
        no strict 'subs';
        $self->{_lchown} = eval { require Lchown } && Lchown::LCHOWN_AVAILABLE;
        
        if(!$self->{_lchown}) {
            if($self->{no_lchown}) {
                warn "Not restoring symlink ownership (LChown is not available)\n";
            }
            else {
                die "Cannot restore symlink ownership (no Lchown module) but --no-lchown option not given\n";
            }
        }
    }

    if (! defined $self->{_utimensat}) {
        no strict 'subs';
        $self->{_utimensat} = eval { require Utimensat } && Utimensat::UTIMENSAT_AVAILABLE;

        if(!$self->{_utimensat}) {
            warn "Not restoring symlink access and modification (utimensat is not available)\n";
        }         
    }
    
    $self->{metafile} = Brackup::DecryptedFile->new($self->{filename});

    # We start with 1 and only use more if one download/decrypt is successful
    Brackup::ProcManager->set_maximum('restore', 1) if $self->{daemons};
    $self->{childerrors} = [];

    return $self;
}

# returns a hashref of { "foo" => "bar" } from { ..., "Driver-foo" => "bar" }
sub _driver_meta {
    my $src = shift;
    my $ret = {};
    foreach my $k (keys %$src) {
        next unless $k =~ /^Driver-(.+)/;
        $ret->{$1} = $src->{$k};
    }
    return $ret;
}

sub _restore_item {
    my $self = shift;
    my $meta = shift;
    my $it = shift;
    
    my $type = $it->{Type} || "f";
    my $path = unprintable($it->{Path});
    my $path_escaped = $it->{Path};
    my $path_escaped_stripped = $it->{Path};
    die "Unknown filetype: type=$type, file: $path_escaped" unless $type =~ /^[ldfpbcs]$/;
    
    if ($self->{prefix}) {
         
        # Skip unless path is prefix or begins with prefix/
        return undef unless $path =~ m/^\Q$self->{prefix}\E(?:\/|$)/;
        
        ###
        # Translate source $path into destination path $full (and $full_escaped, for printing).
         
        # If non-directory, and $path begins with prefix or prefix/, then strip all but last component.
        if ($type ne 'd' && $path =~ m/^\Q$self->{prefix}\E\/?$/) {
            if (my ($leading_prefix) = ($self->{prefix} =~ m/^(.*\/)[^\/]+\/?$/)) {
                $path =~ s/^\Q$leading_prefix\E//;
                $path_escaped_stripped =~ s/^\Q$leading_prefix\E//;
            }
        }
         
        # If directory, strip prefix or prefix/
        else {
            $path =~ s/^\Q$self->{prefix}\E\/?//;
            $path_escaped_stripped =~ s/^\Q$self->{prefix}\E\/?//;
        }
    }
    
    my $full = $self->{to} . "/" . $path;
    my $full_escaped = $self->{to} . "/" . $path_escaped_stripped;
    
    # restore default modes/user/group from header
    $it->{Mode} ||= ($type eq 'd' ? $meta->{DefaultDirMode} : $meta->{DefaultFileMode});
    $it->{UID}  ||= $meta->{DefaultUID};
    $it->{GID}  ||= $meta->{DefaultGID};
    
    warn " * restoring $path_escaped to $full_escaped\n" if $self->{verbose};
    my $err;
    try {
        $self->_restore_link     ($full, $it) if $type eq "l";
        $self->_restore_directory($full, $it) if $type eq "d";
        $self->_restore_fifo     ($full, $it) if $type eq "p";
        $self->_restore_dev      ($full, $it) if $type eq "b";
        $self->_restore_dev      ($full, $it) if $type eq "c";
        $self->__restore_file    ($full, $it) if $type eq "f";
        return undef                          if $type eq "s";
        
        $self->_chown($full, $it, $type, $meta) if defined($it->{UID}) || defined($it->{GID});
        
    } catch {
        die $_ unless $self->{onerror} eq 'continue';
         
        warn $_;
        return $_;
    };
    
    return undef;
}

sub restore {
    my ($self) = @_;
    my $parser = $self->parser;
    my $meta = $parser->readline;
    my $driver_class = $meta->{BackupDriver};
    die "No driver specified" unless $driver_class;

    my $driver_meta = _driver_meta($meta);

    my $confsec;
    if ($self->{config} && $meta->{TargetName}) {
        $confsec = eval { $self->{config}->get_section('TARGET:' . $meta->{TargetName}) };
    }
    # If no config section, use an empty one up with no keys to simplify Target handling
    $confsec ||= Brackup::ConfigSection->new('fake');

    $confsec->parse_globals_for_target();

    eval "use $driver_class; 1;" or die
        "Failed to load driver ($driver_class) to restore from: $@\n";
    my $target = eval {"$driver_class"->new_from_backup_header($driver_meta, $confsec); };
    if ($@) {
        die "Failed to instantiate target ($driver_class) for restore. Perhaps it doesn't support restoring yet?\n\nThe error was: $@";
    }
    $self->{_target} = $target;
    $self->{_meta}   = $meta;

    # handle absolute prefixes by stripping off RootPath to relativise
    if ($self->{prefix} && $self->{prefix} =~ m/^\//) {
        $self->{prefix} =~ s/^\Q$meta->{RootPath}\E\/?//;
    }

    # we first process directories, then files sorted by their first chunk,
    # then the rest. The file sorting allows us to avoid loading composite
    # chunks and identical single chunk files multiple times from the target
    # (see _restore_file)
    my (@dirs, @rest);
    my $files;
    while (my $it = $parser->readline) {
        my $type = $it->{Type} || 'f';
        if($type eq 'f') {
            # find dig of first chunk
            ($it->{Chunks} || '') =~ /^(\S+)/;
            my ($offset, $len, $enc_len, $dig) = split(/;/, $1 || '');
            $dig ||= '';
            $it->{fst_dig} = $dig;
            push @{$files->{$dig}}, $it;
        } elsif($type eq 'd') {
            push @dirs, $it;
        } else {
            push @rest, $it;
        }
    }
    
    my @errors;
    my $restore_count = 0;
    my $err;
    for my $it (@dirs, @rest) {
        $err = $self->_restore_item($meta, $it);
         
        push @errors, $err if defined $err;
         
        $restore_count++;       
    }

    my ($k, $it_array);
    while( ($k, $it_array) = each %$files ) {
        $err = $self->_restore_files($meta, $it_array);
         
        push @errors, @$err if defined $err && ref($err) eq 'ARRAY';
         
        $restore_count += @$it_array;
         
        if($self->{daemons}) {
            Brackup::ProcManager->wait_for_extra_children('restore');
        }
    }
    
    # clear chunk cached by _restore_file
    delete $self->{_cached_dig};
    delete $self->{_cached_dataref};

    Brackup::ProcManager->wait_for_all_children('restore');

    push @errors, @{ $self->{childerrors} };

    if ($restore_count) {
        warn " * fixing stat info\n" if $self->{verbose};
        $self->_exec_statinfo_updates;
        warn " * done\n" if $self->{verbose};
        die \@errors if @errors;
        return 1;
    } else {
        die "nothing found matching '$self->{prefix}'.\n" if $self->{prefix};
        die "nothing found to restore.\n";
    }
}

sub _lookup_remote_uid {
    my ($self, $remote_uid, $meta) = @_;

    return $remote_uid if $self->{numeric_ids};
    
    return $self->{_local_uid_map}->{$remote_uid}
        if defined $self->{_local_uid_map}->{$remote_uid};

    # meta remote user map - remote_uid => remote username
    $self->{_remote_user_map} ||= { map { split /:/, $_, 2 } split /\s+/, $meta->{UIDMap} };

    # try and lookup local uid using remote username
    if (my $remote_user = $self->{_remote_user_map}->{$remote_uid}) {
        my $local_uid = getpwnam($remote_user);
        return $self->{_local_uid_map}->{$remote_uid} = $local_uid
            if defined $local_uid;
    }

    # if remote username missing locally, fallback to $remote_uid
    return $self->{_local_uid_map}->{$remote_uid} = $remote_uid;
}

sub _lookup_remote_gid {
    my ($self, $remote_gid, $meta) = @_;

    return $remote_gid if $self->{numeric_ids};
    
    return $self->{_local_gid_map}->{$remote_gid}
        if defined $self->{_local_gid_map}->{$remote_gid};

    # meta remote group map - remote_gid => remote group
    $self->{_remote_group_map} ||= { map { split /:/, $_, 2 } split /\s+/, $meta->{GIDMap} };

    # try and lookup local gid using remote group
    if (my $remote_group = $self->{_remote_group_map}->{$remote_gid}) {
        my $local_gid = getgrnam($remote_group);
        return $self->{_local_gid_map}->{$remote_gid} = $local_gid
            if defined $local_gid;
    }

    # if remote group missing locally, fallback to $remote_gid
    return $self->{_local_gid_map}->{$remote_gid} = $remote_gid;
}

sub _chown {
    my ($self, $full, $it, $type, $meta) = @_;

    my $uid = defined($it->{UID}) ? $self->_lookup_remote_uid($it->{UID}, $meta) : undef;
    my $gid = defined($it->{GID}) ? $self->_lookup_remote_gid($it->{GID}, $meta) : undef;

    if ($type eq 'l') {
        if ($self->{_lchown}) {
            Lchown::lchown($uid, -1, $full) if defined $uid;
            Lchown::lchown(-1, $gid, $full) if defined $gid;
        }
    } else {
        # ignore errors, but change uid and gid separately to sidestep unprivileged failures
        chown $uid, -1, $full if defined $uid;
        chown -1, $gid, $full if defined $gid;
    }
}

sub _update_statinfo {
    my ($self, $full, $it) = @_;

    my $sub;
    if( $self->{_utimensat} && defined($it->{Type}) && $it->{Type} eq 'l' && $full =~ m!^/!s ) {

        $sub = sub {
            if (defined($it->{Mtime})) {
                Utimensat::utimensat($it->{Atime} // $it->{Mtime}, $it->{Mtime}, 1, $full) || 
                    die "Failed to change modification time of symlink $full: $!";
            }
        };
    }
    else {

        $sub = sub {
            if (defined $it->{Mode}) {
                chmod(oct $it->{Mode}, $full) or
                    die "Failed to change mode of $full: $!";
            }

            if ($it->{Mtime} || $it->{Atime}) {
                utime($it->{Atime} || $it->{Mtime},
                $it->{Mtime} || $it->{Atime},
                $full) or
                    die "Failed to change utime of $full: $!";
            }
        };
    }
    
    push @{ $self->{_stats_to_run} }, $sub;
}

sub _exec_statinfo_updates {
    my $self = shift;

    # change the modes/times in backwards order, going from deep
    # files/directories to shallow ones.  (so we can reliably change
    # all the directory mtimes without kernel doing it for us when we
    # modify files deeper)
    while (my $sb = pop @{ $self->{_stats_to_run} }) {
        $sb->();
    }
}

# Check if $self->{conflict} setting allows us to skip this item
sub _can_skip {
    my ($self, $full, $it) = @_;

    if ($self->{conflict} eq 'skip') {
        return 1;
    } elsif ($self->{conflict} eq 'overwrite') {
        return 0;
    } elsif ($self->{conflict} eq 'update') {
        my $st = stat $full
            or die "stat on '$full' failed: $!\n";
        return 1 if defined $it->{Mtime} && $st->mtime >= $it->{Mtime};
    }
    else {
        die "Invalid '--conflict' setting '$self->{conflict}'\n";
    }
    return 0;
}

sub _restore_directory {
    my ($self, $full, $it) = @_;

    # Apply conflict checks to directories
    if (-d $full && $self->{conflict} ne 'abort') {
        return if $self->_can_skip($full, $it);
    }

    unless (-d $full) {
        mkdir $full or    # FIXME: permissions on directory
            die "Failed to make directory: $full ($it->{Path}): $!";
    }

    $self->_update_statinfo($full, $it);
}

sub _restore_link {
    my ($self, $full, $it) = @_;

    if (-e $full) {
        die "Link $full ($it->{Path}) already exists.  Aborting."
            if $self->{conflict} eq 'abort';
        return if $self->_can_skip($full, $it);

        # Can't overwrite symlinks, so unlink explicitly if we're not skipping
        unlink $full
            or die "Failed to unlink link $full: $!";
    }

    my $link = unprintable($it->{Link});
    symlink $link, $full or
        die "Failed to link $full: $!";
}

sub _restore_fifo {
    my ($self, $full, $it) = @_;

    if (-e $full) {
        die "Named pipe/fifo $full ($it->{Path}) already exists.  Aborting."
            if $self->{conflict} eq 'abort';
        return if $self->_can_skip($full, $it);

        # Can't overwrite fifos, so unlink explicitly if we're not skipping
        unlink $full
            or die "Failed to unlink fifo $full: $!";
    }

    mkfifo($full, $it->{Mode}) or die "mkfifo failed: $!";

    $self->_update_statinfo($full, $it);
}

sub _restore_dev {
    my ($self, $full, $it) = @_;

    if (-e $full) {
        return if $self->_can_skip($full, $it, 'Device');

        # Can't overwrite fifos, so unlink explicitly if we're not skipping
        unlink $full
            or die "Failed to unlink fifo $full: $!";
    }

    my @type = split(';', $it->{'Device'});
    
    # Returns 0 on success, -1 on failure
    Unix::Mknod::mknod($full, ($it->{Type} eq 'c' ? S_IFCHR : S_IFBLK) | oct($it->{Mode}), Unix::Mknod::makedev($type[0], $type[1])) && die "mknod failed: $!";

    $self->_update_statinfo($full, $it);
}

sub _restore_files {
    my ($self, $meta, $it_array) = @_;

    unless( $self->{daemons} ){
        return $self->__restore_files($meta, $it_array);
    }

    Brackup::ProcManager->start_child('restore', $self, 'restore_daemon_handler', [$meta, $it_array]);
    
     return undef;
}

sub restore_daemon_handler {
    my ($self, $flag, $data) = @_;

    if($flag eq 'inchild'){

        if(eval {
            my $err = $self->__restore_files( @{ $data->{data} } );
                print join("\n", @$err) if defined $err && ref($err) eq 'ARRAY';
            1;
        }){
            return 0;
        }
        print $@; # Sending error to parent
        return -1;

    }
    elsif($flag eq 'childexit'){

        my $code = $data->{retcode};
        my $fh = $data->{fh};
        local $/;
        my $r = <$fh>;
        if($code != 0){
            die "Restore daemon returned '$r' with code '$code' PID '$data->{pid}'" unless $self->{onerror} eq 'continue';
            push @{ $self->{childerrors} }, split("\n",$r) if $code != 0;
        }
        else{
            # Now we can use all threads
            Brackup::ProcManager->set_maximum('restore', $self->{daemons});
        }

    }
}

sub __restore_files {
    my ($self, $meta, $it_array) = @_;
    
    my @errors;
    my $err;
    foreach my $it (@$it_array) {
        $err = $self->_restore_item($meta, $it);
        
        push @errors, $err if defined $err;
    }
    
    return \@errors;
}

sub __restore_file {
    my ($self, $full, $it) = @_;

    if (-e $full && -s $full) {
        die "File $full ($it->{Path}) already exists.  Aborting."
            if $self->{conflict} eq 'abort';
        return if $self->_can_skip($full, $it);
    }
    # If $full exists, unlink (in case readonly when overwriting would fail)
    unlink $full if -e $full;

    sysopen(my $fh, $full, O_CREAT|O_WRONLY|O_TRUNC) or die "Failed to open '$full' for writing: $!";
    binmode($fh);
    my @chunks = grep { $_ } split(/\s+/, $it->{Chunks} || "");
    foreach my $ch (@chunks) {
        # {METASYNTAX}  (search for this label to see where else this syntax is used)
        my ($offset, $len, $enc_len, $dig) = split(/;/, $ch);

        # we process files sorted by the dig of their first chunk, caching
        # the last seen chunk to avoid loading composite chunks multiple
        # times (all files included in composite chunks are single-chunk
        # files, by definition). Even for non-composite chunks there is a
        # speedup if we have single-chunk identical files.
        my $dataref;
        if($dig eq ($self->{_cached_dig} || '')) {
            warn "   ** using cached chunk $dig\n" if $self->{verbose};
            $dataref = $self->{_cached_dataref};
        } else {
            warn "   ** loading chunk $dig from target\n" if $self->{verbose};
            $dataref = $self->{_target}->load_chunk($dig) or
                die "Error loading chunk $dig from the restore target\n";
            $self->{_cached_dig} = $dig;
            $self->{_cached_dataref} = $dataref;
        }

        my $len_chunk = length $$dataref;

        # using just a range of the file
        if ($enc_len =~ /^(\d+)-(\d+)$/) {
            my ($from, $to) = ($1, $2);
            # file range.  gotta be at least as big as bigger number
            unless ($len_chunk >= $to) {
                die "Backup chunk $dig isn't at least as big as range: got $len_chunk, needing $to\n";
            }
            my $region = substr($$dataref, $from, $to-$from);
            $dataref = \$region;
        } else {
            # using the whole chunk, so make sure fetched size matches
            # expected size
            unless ($len_chunk == $enc_len) {
                die "Backup chunk $dig isn't of expected length: got $len_chunk, expecting $enc_len\n";
            }
        }

        my $decrypted_ref = Brackup::Decrypt::decrypt_data($dataref, meta => $self->{_meta});
        print $fh $$decrypted_ref;
    }
    close($fh) or die "Close failed";

    if (my $good_dig = $it->{Digest}) {
        die "not capable of verifying digests of from anything but sha1"
            unless $good_dig =~ /^sha1:(.+)/;
        $good_dig = $1;

        sysopen(my $readfh, $full, O_RDONLY) or die "Failed to reopen '$full' for verification: $!";
        binmode($readfh);
        my $sha1 = Digest::SHA1->new;
        $sha1->addfile($readfh);
        my $actual_dig = $sha1->hexdigest;

        unless ($actual_dig eq $good_dig || $full =~ m!\.brackup-digest\.db\b!) {
            die "Digest of restored file ($full) doesn't match:\n  Got:      $actual_dig\n  Expected: $good_dig\n";
        }
    }

    $self->_update_statinfo($full, $it);
}

# returns iterator subref which returns hashrefs or undef on EOF
sub parser {
    my $self = shift;
    return Brackup::Metafile->open($self->{metafile}->name);
}

1;

