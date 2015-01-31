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

package Brackup::Backup;
use strict;
use warnings;
use Carp qw(croak);
use Brackup::ChunkIterator;
use Brackup::CompositeChunk;
use Brackup::GPGProcManager;
use Brackup::GPGProcess;
use Brackup::Webhook;
use Brackup::Util qw(noclobber_filename);
use File::Basename;
use File::Spec;
use File::Temp qw(tempfile);

sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;

    $self->{root}    = delete $opts{root};     # Brackup::Root
    $self->{target}  = delete $opts{target};   # Brackup::Target
    $self->{dryrun}  = delete $opts{dryrun};   # bool
    $self->{verbose} = delete $opts{verbose};  # bool
    $self->{inventory} = delete $opts{inventory};  # bool
    $self->{savefiles} = delete $opts{savefiles};  # bool
    $self->{zenityprogress} = delete $opts{zenityprogress};  # bool
    $self->{arguments} = delete $opts{arguments};

    $self->{modecounts} = {}; # type -> mode(octal) -> count
    $self->{idcounts}   = {}; # type -> uid/gid -> count

    $self->{_uid_map} = {};   # uid -> username
    $self->{_gid_map} = {};   # gid -> group

    $self->{saved_files} = [];   # list of Brackup::File objects backed up
    $self->{unflushed_files} = [];   # list of Brackup::File objects not in backup_file

    croak("Unknown options: " . join(', ', keys %opts)) if %opts;

    return $self;
}

# returns true (a Brackup::BackupStats object) on success, or dies with error
sub backup {
    my ($self, $meta_dir) = @_;

    my $root   = $self->{root};
    my $target = $self->{target};

    my $stats  = Brackup::BackupStats->new(arguments => $self->{arguments});

    my @gpg_rcpts = $self->{root}->gpg_rcpts;

    my $n_kb         = 0.0; # num:  kb of all files in root
    my $n_files      = 0;   # int:  # of files in root
    my $n_kb_done    = 0.0; # num:  kb of files already done with (uploaded or skipped)

    # if we're pre-calculating the amount of data we'll
    # actually need to upload, store it here.
    my $n_files_up   = 0;
    my $n_kb_up      = 0.0;
    my $n_kb_up_need = 0.0; # by default, not calculated/used.

    my $n_files_done = 0;   # int
    my @files;         # Brackup::File objs

    my $backup_file; # filename of the metafile
    my $error_to_note;
    my $error_to_return;

    $self->debug("Discovering files in ", $root->path, "...\n");
    $self->report_progress(0, "Discovering files in " . $root->path . "...");
    $root->foreach_file(sub {
        my ($file) = @_;  # a Brackup::File
        push @files, $file;
        $self->record_mode_ids($file);
        $n_files++;
        $n_kb += $file->size / 1024;
    });

    $self->debug("Number of files: $n_files\n");
    $stats->timestamp('File Discovery');
    $stats->set(files_checked_count => $n_files, label => 'Number of Files');
    $stats->set(files_checked_size  => sprintf('%0.01f', $n_kb / 1024), label => 'Total File Size', units => 'MB');

    # calc needed chunks
    if ($ENV{CALC_NEEDED}) {
        my $fn = 0;
        foreach my $f (@files) {
            $fn++;
            if ($fn % 100 == 0) { warn "$fn / $n_files ...\n"; }
            foreach my $pc ($f->chunks) {
                if ($target->stored_chunk_from_inventory($pc)) {
                    $pc->forget_chunkref;
                    next;
                }
                $n_kb_up_need += $pc->length / 1024;
                $pc->forget_chunkref;
            }
        }
        warn "kb need to upload = $n_kb_up_need\n";
        $stats->timestamp('Calc Needed');
    }


    my $chunk_iterator = Brackup::ChunkIterator->new(\@files);
    # undef @files; # DO NOT delete @files as ChunkIterator uses it!
    $stats->timestamp('Chunk Iterator');

    my $gpg_iter;
    my $gpg_pm;   # gpg ProcessManager
    if (@gpg_rcpts) {
        ($chunk_iterator, $gpg_iter) = $chunk_iterator->mux_into(2);
        $gpg_pm = Brackup::GPGProcManager->new($gpg_iter, $target);
    }

    # begin temp backup_file
    my ($metafh, $meta_filename);
    unless ($self->{dryrun}) {

        # A temporary filename. We will finalise the name of the metafile later
        $backup_file = File::Spec->catfile($meta_dir || '', $self->backup_time_str . '.brackup_tmp');

        ($metafh, $meta_filename) = tempfile(
                                             '.' . basename($backup_file) . 'XXXXX',
                                             DIR => dirname($backup_file),
        );

        # Always compress metafile when Gzip available, even when encrypting.
        # Although GPG compresses the data it encrypts, pre-compressing the data carries little or no
        # overhead and ensures that local unencrypted copies of the metafiles are always compressed,
        # after generation by brackup or following retrieval from the target.
        # Previously only local copies of metafiles stored to an unencrypted target would be compressed,
        # which was inconsistent and meant local unencrypted copies of metafiles could occupy excessive
        # amounts of disk space.
        if (eval { require IO::Compress::Gzip }) {
            close $metafh;
            $metafh = IO::Compress::Gzip->new($meta_filename)
                or die "Cannot open tempfile with IO::Compress::Gzip: $IO::Compress::Gzip::GzipError";
        }
        print $metafh $self->backup_header($n_files);
    }

    my $cur_file; # current (last seen) file
    my $cur_file_not_available; # True if errors occurred while reading from the current file
    my @stored_chunks;
    my $file_has_shown_status = 0;

    my $merge_under = $root->merge_files_under;
    my $comp_chunk  = undef;

    my $end_file = sub {
        return unless $cur_file;
        return if $cur_file_not_available;
        if ($merge_under && $comp_chunk) {
            # defer recording to backup_file until CompositeChunk finalization
            $self->add_unflushed_file($cur_file, [ @stored_chunks ]);
        }
        else {
            print $metafh $cur_file->as_rfc822([ @stored_chunks ], $self) if $metafh;
        }
        $self->add_saved_file($cur_file, [ @stored_chunks ]) if $self->{savefiles};
        $n_files_done++;
        $n_kb_done += $cur_file->size / 1024;
        $cur_file = undef;
        $cur_file_not_available = undef;
    };

    my $show_status = sub {
        # use either size of files in normal case, or if we pre-calculated
        # the size-to-upload (by looking in inventory, then we'll show the
        # more accurate percentage)
        my $percdone = 100 * ($n_kb_up_need ?
                              ($n_kb_up / $n_kb_up_need) :
                              ($n_kb_done / $n_kb));
        my $mb_remain = ($n_kb_up_need ?
                         ($n_kb_up_need - $n_kb_up) :
                         ($n_kb - $n_kb_done)) / 1024;

        $self->debug(sprintf("* %-60s %d/%d (%0.02f%%; remain: %0.01f MB)",
                             $cur_file->path, $n_files_done, $n_files, $percdone,
                             $mb_remain));

        $self->report_progress($percdone);
    };

    # Returns if we should continue
    my $start_file = sub {
        $end_file->();

        if($Brackup::Util::SHUTDOWN_REQUESTED){
            $error_to_note = 'stopped'; # add this to the meta file name {METANAME}
            return 0;
        }

        $cur_file = shift;
        $cur_file_not_available = undef;
        @stored_chunks = ();
        $show_status->() if $cur_file->is_dir;
        if ($gpg_iter) { # $gpg_iter is a chunk iterator
            # catch our gpg iterator up.  we want it to be ahead of us,
            # nothing iteresting is behind us.
            $gpg_iter->next while $gpg_iter->behind_by > 1;
        }
        $file_has_shown_status = 0;
        return 1;
    };

    # records are either Brackup::File (for symlinks, directories, etc), or
    # PositionedChunks, in which case the file can asked of the chunk
    while (my $rec = $chunk_iterator->next) {
        my $eval_r = eval {
            # Return values:
            # 1 - OK
            # 2 - call next (skip any statements after the eval)
            # 3 - call last

            if ($rec->isa("Brackup::File")) { # symlinks, directories, etc.
                $start_file->($rec);
                return 2;
            }
            my $pchunk = $rec;
            if ($pchunk->file != $cur_file) {
                unless( $start_file->($pchunk->file) ){
                    warn "Signal received previously. Aborting now...\n";
                    return 3;
                }
            }

            # WARNING The checks here are coupled to the ones in GPGProcManager::next_chunk_to_encrypt

            # have we already stored this chunk before?  (iterative backup)
            my $schunk;
            if ($schunk = $target->stored_chunk_from_inventory($pchunk)) {
                $pchunk->forget_chunkref;
                push @stored_chunks, $schunk;
                $self->debug_more('  * chunk already stored: ', $pchunk->as_string, "\n");
                return 2;
            }

            # weird case... have we stored this same pchunk digest in the
            # current comp_chunk we're building?  these aren't caught by
            # the above inventory check, because chunks in a composite
            # chunk aren't added to the inventory until after the the composite
            # chunk has fully grown (because it's not until it's fully grown
            # that we know the handle for it, its digest)
            if ($comp_chunk && ($schunk = $comp_chunk->stored_chunk_from_dup_internal_raw($pchunk))) {
                $pchunk->forget_chunkref;
                ## ! $schunk in a composite chunk returns the digest of the composite chunk
                push @stored_chunks, $schunk;
                $self->debug_more('  * component chunk already stored: ', $pchunk->as_string, "\n");
                return 2;
            }

            # Check if there are any target daemons currently storing the $pchunk
            if( $schunk = $target->is_pchunk_being_stored($pchunk) ){
                $pchunk->forget_chunkref;
                ## ! $schunk in a composite chunk returns the digest of the composite chunk
                push @stored_chunks, $schunk;
                $self->debug_more('  * chunk is already being stored by another daemon: ', $pchunk->as_string, "\n");
                return 2;
            }

            unless ($file_has_shown_status++) {
                $show_status->();
                $n_files_up++;
            }
            $self->debug("  * storing chunk: ", $pchunk->as_string, "\n");
            $self->report_progress(undef, $pchunk->file->path . " (" . $pchunk->offset . "," . $pchunk->length . ")");

            unless ($self->{dryrun}) {
                $schunk = Brackup::StoredChunk->new($pchunk);

                # encrypt it
                if (@gpg_rcpts) {
                    $self->debug_more("    * encrypting ... \n");
                    $schunk->set_encrypted_chunkref($gpg_pm->enc_chunkref_of($pchunk));
                }

                # see if we should pack it into a bigger blob
                my $chunk_size = $schunk->backup_length;

                # see if we should merge this chunk (in this case, file) together with
                # other small files we encountered earlier, into a "composite chunk",
                # to be stored on the target in one go.

                # WARNING Enabling the below would probably make it impossible to reconstruct
                #   the inventory from the meta-files. See {MISSINGPDIGEST}
                # Note: no technical reason for only merging small files (is_entire_file),
                # and not the tails of larger files.  just don't like the idea of files being
                # both split up (for big head) and also merged together (for little end).
                # would rather just have 1 type of magic per file.  (split it or join it)
                if ($merge_under && $chunk_size < $merge_under && $pchunk->is_entire_file) {
                    if ($comp_chunk && ! $comp_chunk->can_fit($chunk_size)) {
                        $self->debug("Finalizing composite chunk $comp_chunk...");
                        $comp_chunk->finalize;
                        $comp_chunk = undef;
                        $self->flush_files($metafh);
                    }
                    $comp_chunk ||= Brackup::CompositeChunk->new($root, $target);
                    $self->debug_more("    * appending to composite chunk ... \n");
                    $comp_chunk->append_little_chunk($schunk);
                } else {
                    # store it regularly, as its own chunk on the target
                    $self->debug_more("    * storing ... \n");
                    $target->store_chunk($schunk)
                        or die "Chunk storage failed.\n";
                    $self->debug_more("    * chunk stored\n");
                }

                # if only this worked... (LWP protocol handler seems to
                # get confused by its syscalls getting interrupted?)
                #local $SIG{CHLD} = sub {
                #    print "some child finished!\n";
                #    $gpg_pm->start_some_processes;
                #};


                $n_kb_up += $pchunk->length / 1024;
                $schunk->forget_chunkref;
                push @stored_chunks, $schunk;
            }

            #$stats->note_stored_chunk($schunk);

            # DEBUG: verify it got written correctly
            if ($ENV{BRACKUP_PARANOID}) {
                die "FIX UP TO NEW API";
                #my $saved_ref = $target->load_chunk($handle);
                #my $saved_len = length $$saved_ref;
                #unless ($saved_len == $chunk->backup_length) {
                #    warn "Saved length of $saved_len doesn't match our length of " . $chunk->backup_length . "\n";
                #    die;
                #}
            }

            $stats->check_maxmem;
            $pchunk->forget_chunkref;

            return 1;
        };

        if($eval_r){
            next if $eval_r == 2;
            last if $eval_r == 3;
        }
        else{ # Error occurred
            my $err = $@;
            if($err =~ /^\[SKIP_FILE\] (.*)$/){ # Thrown when we fail to open a file - it might have been deleted in the meantime

                warn "Skipped storing the current chunk because '$1'\n";
                $cur_file_not_available = 1; # This prevents end_file from adding the file to be flushed to the metafile

            }else{ # A serious error occurred

                warn "*** ERROR occurred: '$err' Attempting to flush metafile...\n";
                $error_to_note = 'with-error'; # add this to the meta file name {METANAME}
                $error_to_return = $err;
                $cur_file_not_available = 1; # This prevents end_file from adding the file to be flushed to the metafile
                last;

            }
        }

    } # end while

    # If using daemonised storage, ensure all chunks are stored and added to the inventory
    $target->wait_for_kids();

    $end_file->();

    $comp_chunk->finalize if $comp_chunk;
    $target->wait_for_kids(); # finalize() calls store_chunk, so need to wait again

    $stats->timestamp('Chunk Storage');
    $self->debug('Flushing files to metafile');
    $self->flush_files($metafh);
    $stats->timestamp('Metafile Final Flush');
    $stats->set(files_uploaded_count => $n_files_up, label => 'Number of Files Uploaded');
    $stats->set(files_uploaded_size  => sprintf('%0.01f', $n_kb_up / 1024), label => 'Total File Size Uploaded', units => 'MB');

    unless ($self->{dryrun}) {

        close $metafh or die "Close on metafile '$backup_file' failed: $!";

        # Finalising the name of the meta-file
        # {METANAME}
        my $meta_name = $self->{root}->publicname . "-" . $self->{target}->name . "-" . $self->backup_time_str #
            . ( $error_to_note ? '.' . $error_to_note : '' )  #
            . '.brackup';
        $backup_file = File::Spec->catfile($meta_dir || '', $meta_name);
        $backup_file = noclobber_filename($backup_file); # just in case

        rename $meta_filename, $backup_file
            or die "Failed to rename temporary backup_file: $!\n";

        my ($store_fh, $store_filename);
        my $is_encrypted = 0;

        # store the metafile, encrypted, on the target
        if (@gpg_rcpts) {
            my $encfile = $backup_file . ".enc";
            my @recipients = map {("--recipient", $_)} @gpg_rcpts;
            system($self->{root}->gpg_path, $self->{root}->gpg_args,
                   @recipients,
                   "--trust-model=always",
                   "--batch",
                   "--encrypt",
                   "--output=$encfile",
                   "--yes",
                   $backup_file)
                and die "Failed to run gpg while encryping metafile: $!\n";
            open ($store_fh, $encfile) or die "Failed to open encrypted metafile '$encfile': $!\n";
            $store_filename = $encfile;
            $is_encrypted = 1;
        } else {
            # Reopen $metafh to reset file pointer (no backward seek with IO::Compress::Gzip)
            open($store_fh, $backup_file) or die "Failed to open metafile '$backup_file': $!\n";
            $store_filename = $backup_file;
        }

        # store it on the target
        $self->debug("Storing metafile to " . ref($target));
        $target->store_backup_meta($meta_name, $store_fh, { filename => $store_filename, is_encrypted => $is_encrypted });
        $stats->timestamp('Metafile Storage');

        # cleanup encrypted metafile
        if ($is_encrypted) {
            close $store_fh or die "Close on encrypted metafile failed: $!";
            unlink $store_filename;
        }

        $target->cleanup();
    }

    $self->report_progress(100, "Backup complete.");

    if (my $url = $root->webhook_url) {
        Brackup::Webhook->new(url => $url, root => $root, target => $target, stats => $stats)->fire;
    }

    return ($stats, $backup_file, $error_to_return);
}

sub default_file_mode {
    my $self = shift;
    return $self->{_def_file_mode} ||= $self->_default_mode('f');
}

sub default_directory_mode {
    my $self = shift;
    return $self->{_def_dir_mode} ||= $self->_default_mode('d');
}

sub _default_mode {
    my ($self, $type) = @_;
    # Provide a default, default mode of 0000 if none can be calculated.
    my $map = $self->{modecounts}{$type} || { '0000' => 0 };
    return (sort { $map->{$b} <=> $map->{$a} } keys %$map)[0];
}

sub default_uid {
    my $self = shift;
    return $self->{_def_uid} ||= $self->_default_id('u');
}

sub default_gid {
    my $self = shift;
    return $self->{_def_gid} ||= $self->_default_id('g');
}

sub _default_id {
    my ($self, $type) = @_;
    my $map = $self->{idcounts}{$type} || {};
    return (sort { $map->{$b} <=> $map->{$a} } keys %$map)[0];
}

# space-separated list of local uid:username mappings
sub uid_map {
    my $self = shift;
    my @map;
    my $uidcounts = $self->{idcounts}{u};
    for my $uid (sort { $a <=> $b } keys %$uidcounts) {
      if (my $name = getpwuid($uid)) {
        push @map, "$uid:$name";
      }
    }
    return join(' ', @map);
}

# space-separated list of local gid:group mappings
sub gid_map {
    my $self = shift;
    my @map;
    my $gidcounts = $self->{idcounts}{g};
    for my $gid (sort { $a <=> $b } keys %$gidcounts) {
      if (my $name = getgrgid($gid)) {
        push @map, "$gid:$name";
      }
    }
    return join(' ', @map);
}

sub backup_time {
    my $self = shift;
    return $self->{backup_time} ||= time();
}

sub backup_time_str {
    return Brackup::Util::unix2human( $_[0]->backup_time() );
}

sub backup_header {
    my $self = shift;
    my $files = shift;

    my $ret = "";
    my $now = $self->backup_time;
    $ret .= "BackupTime: " . $now . " (" . $self->backup_time_str . ")\n";
    $ret .= "BackupDriver: " . ref($self->{target}) . "\n";
    if (my $fields = $self->{target}->backup_header) {
        foreach my $k (sort keys %$fields) {
            die "Bogus header field from driver" unless $k =~ /^\w+$/;
            my $val = $fields->{$k};
            next if ! defined $val || $val eq '';   # skip keys with empty values
            die "Bogus header value from driver" if $val =~ /[\r\n]/;
            $ret .= "Driver-$k: $val\n";
        }
    }
    $ret .= "RootName: " . $self->{root}->name . "\n";
    $ret .= "RootPath: " . $self->{root}->path . "\n";
    $ret .= "TargetName: " . $self->{target}->name . "\n";
    $ret .= "DefaultFileMode: " . $self->default_file_mode . "\n";
    $ret .= "DefaultDirMode: " . $self->default_directory_mode . "\n";
    $ret .= "DefaultUID: " . $self->default_uid . "\n";
    $ret .= "DefaultGID: " . $self->default_gid . "\n";
    $ret .= "UIDMap: " . $self->uid_map . "\n";
    $ret .= "GIDMap: " . $self->gid_map . "\n";
    $ret .= "GPG-Recipient: $_\n" for $self->{root}->gpg_rcpts;
    $ret .= "FileCount: $files\n";
    $ret .= "\n";
    return $ret;
}

sub record_mode_ids {
    my ($self, $file) = @_;
    $self->{modecounts}{$file->type}{$file->mode}++;
    $self->{idcounts}{u}{$file->uid}++;
    $self->{idcounts}{g}{$file->gid}++;
}

sub add_unflushed_file {
    my ($self, $file, $handlelist) = @_;
    push @{ $self->{unflushed_files} }, [ $file, $handlelist ];
}

sub flush_files {
    my ($self, $fh) = @_;
    while (my $rec = shift @{ $self->{unflushed_files} }) {
      next unless $fh;
      my ($file, $stored_chunks) = @$rec;
      print $fh $file->as_rfc822($stored_chunks, $self);
    }
}

sub add_saved_file {
    my ($self, $file, $handlelist) = @_;
    push @{ $self->{saved_files} }, [ $file, $handlelist ];
}

sub foreach_saved_file {
    my ($self, $cb) = @_;
    foreach my $rec (@{ $self->{saved_files} }) {
        $cb->(@$rec);  # Brackup::File, arrayref of Brackup::StoredChunk
    }
}

sub debug {
    my ($self, @m) = @_;
    return unless $self->{verbose};
    my $line = join("", @m);
    chomp $line;
    print $line, "\n";
}

sub debug_more {
    my $self = shift;
    return unless $self->{verbose} && $self->{verbose} >= 2;
    # not found: $self->report_open_files;
    $self->debug(@_);
}

sub report_progress {
    my ($self, $percent, $message) = @_;

    if ($self->{zenityprogress}) {
        if (defined($message) && length($message) > 100) {
            $message = substr($message, 0, 100)."...";
        }
        print STDOUT "#", $message, "\n" if defined $message;
        print STDOUT $percent, "\n" if defined $percent;
    }
}

1;

