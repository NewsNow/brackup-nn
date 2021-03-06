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

package Brackup::Target;

use strict;
use warnings;
use Brackup::InventoryDatabase;
use Brackup::TargetBackupStatInfo;
use Brackup::Util 'tempfile';
use Brackup::DecryptedFile;
use Brackup::ProcManager;
use Carp qw(croak);

use Time::HiRes qw(time sleep);

sub new {
    my ($class, $confsec, $opts) = @_;
    my $self = bless {}, $class;
    $self->{name} = $confsec->name;
    $self->{name} =~ s/^TARGET://
        or die "No target found matching " . $confsec->name;
    die "Target name must be only a-z, A-Z, 0-9, and _."
        unless $self->{name} =~ /^\w+/;

    $self->{keep_backups} = $confsec->value("keep_backups");
    $self->{thinning} = $confsec->value("thinning");
    $self->{inv_db} =
        Brackup::InventoryDatabase->new($confsec->value("inventorydb_file") ||
                                        $confsec->value("inventory_db") ||
                                        "$ENV{HOME}/.brackup-target-$self->{name}.invdb",
                                        $confsec,
                                        $opts->{allow_new_inv});

    $self->{verbose} = $opts->{verbose};
    $self->{daemons} = $confsec->value("daemons") || '0';
    
    # Reads the 'threads' option, and then check for existence of threads module, so that
    # an error is not reported about "Unknown config params" where threads are not available
    # but option is specified in .brackup.conf
    $self->{threads} = (defined($confsec->value("threads")) ? $confsec->value("threads") : 4);
    $self->{threads} = 0 unless eval "use threads; use threads::shared; 1;";
    
    $self->{gpg_daemons} = $confsec->value("gpg_daemons") || '5';
    $self->{local_meta_dir}    = $confsec->value('local_meta_dir');

    $self->{childgroup} = ref($class) ? ref($class) : $class;
    Brackup::ProcManager->set_maximum($self->{childgroup}, $self->{daemons});

    return $self;
}

sub name {
    my $self = shift;
    return $self->{name};
}

# return hashref of key/value pairs you want returned to you during a restore
# you should include anything you need to restore.
# keys must match /^\w+$/
sub backup_header {
    return {}
}

# returns bool
sub has_chunk {
    my ($self, $chunk) = @_;
    die "ERROR: has_chunk not implemented in sub-class $self";
}

# returns a chunk reference on success, or returns false or dies otherwise
sub load_chunk {
    my ($self, $dig) = @_;
    die "ERROR: load_chunk not implemented in sub-class $self";
}

# returns true on success, or returns false or dies otherwise.
sub store_chunk {
    my ($self, $chunk) = @_;
    die "ERROR: store_chunk not implemented in sub-class $self";
}

# Call this from the implementation of store_chunk to use parallelised storing
# In this case, put the storing logic in _store_chunk in the subclass
sub daemonised_store_chunk {
    my ($self, $schunk) = @_;

    if(!$self->{daemons}) {
        if($self->_store_chunk($schunk)) {
            $schunk->add_me_to_inventory($self);
            return 1;
        }
        else {
           return 0;
        }
    }

    Brackup::ProcManager->start_child($self->{childgroup}, $self, 'store_daemon_handler', {'schunk'=>$schunk});

    return 1;
}

sub store_daemon_handler {
    my ($self, $flag, $data) = @_;

    if($flag eq 'inchild'){

        my $schunk = $data->{data}->{schunk};
        $0 .= " Storing schunk " . $schunk->backup_digest;
        return $self->_store_chunk($schunk) ? 0 : -1; # process return code

    }
    elsif($flag eq 'childexit'){

        if($data->{retcode}==0){
            $data->{data}->{schunk}->add_me_to_inventory($self);
        }
        else{
            die "Failed to store chunk\n";
        }

    }
}

sub _store_chunk {
    my ($self, $chunk) = @_;
    die "ERROR: store_chunk not implemented in sub-class $self";
}

sub wait_for_kids {
    my $self = shift;
    Brackup::ProcManager->wait_for_all_children( $self->{childgroup} );
}

# returns true on success, or returns false or dies otherwise.
sub delete_chunk {
    my ($self, $chunk) = @_;
    die "ERROR: delete_chunk not implemented in sub-class $self";
}

# returns true on success, or returns false or dies otherwise.
# in absence of sub-class override, simply wraps delete_chunk
sub delete_chunk_multi {
    my ($self, $chunk) = @_;
    $self->delete_chunk($chunk);
}

# returns true on success, or returns false or dies otherwise.
# in absence of sub-class override, does nothing.
sub delete_chunks_multi {
    return 1;
}

# returns a list of names of all chunks
sub chunks {
    my ($self) = @_;
    die "ERROR: chunks not implemented in sub-class $self";
}

# returns a hash of chunks, keyed on name, value indicating chunk size
sub chunks_with_length {
    my ($self) = @_;
    die "ERROR: chunks not implemented in sub-class $self";
}

sub inventory_db {
    my $self = shift;
    return $self->{inv_db};
}

sub add_to_inventory {
    my ($self, $pchunk, $schunk) = @_;
    my $key  = $pchunk->inventory_key;
    my $db = $self->inventory_db;
    $db->set($key => $schunk->inventory_value);
}

# Check if a child is already storing a positioned chunk
sub is_pchunk_being_stored {
    my $self = shift;
    my $pchunk = shift;

    return undef unless $self->{daemons};
    my $schunk;

    Brackup::ProcManager->for_each_child( $self->{childgroup}, sub{
        if($schunk = $_[0]->{data}->{schunk}->has_pchunk($pchunk)){
            return 0;
        }
        return 1;
    });

    return $schunk;
}

# return stored chunk, given positioned chunk, or undef.  no
# need to override this, unless you have a good reason.
sub stored_chunk_from_inventory {
    my ($self, $pchunk) = @_;
    my $key    = $pchunk->inventory_key;
    my $db     = $self->inventory_db;
    my $invval = $db->get($key)
        or return undef;
    return Brackup::StoredChunk->new_from_inventory_value($pchunk, $invval);
}

# return a list of TargetBackupStatInfo objects representing the
# stored backup metafiles on this target.
sub backups {
    my ($self) = @_;
    die "ERROR: backups method not implemented in sub-class $self";
}

# downloads the given backup name to the current directory (with
# *.brackup extension)
sub get_backup {
    my ($self, $name) = @_;
    die "ERROR: get_backup method not implemented in sub-class $self";
}

sub delete_backup_and_local_metafile {
    my ($self, $name, $meta_dir) = @_;

    if($meta_dir){
        my $f = File::Spec->catfile($meta_dir, $name);
        unlink $f || warn "Could not delete local metafile '$f'\n";
    }

    return $self->delete_backup($name);
}

# deletes the given backup from this target
sub delete_backup {
    my ($self, $name) = @_;
    die "ERROR: delete_backup method not implemented in sub-class $self";
}

# cleanup the given backup from this target
sub cleanup {
}

# removes old metafiles from this target
sub prune {
    my ($self, %opt) = @_;

    my $keep_backups = defined $opt{keep_backups} ? $opt{keep_backups} : $self->{keep_backups};
    my $thinning = $self->{thinning};
    die "ERROR: keep_backups or thinning option not set\n" if ! defined $keep_backups and ! defined $thinning;
    die "ERROR: keep_backups option must be at least 1 if no source is specified\n"
        if( ! $opt{source} and ! $thinning and $keep_backups < 1 );

    # The thinning algorithm searches for a complete backup in the time range
    # desiredgap * ( 1 - thinning_fuzziness ) .. desiredgap * ( 1 + thinning_fuzziness ),
    # where desiredgap is the time gap following the previous backup to keep, retrieved
    # from the thinning config based on the age of the backups.
    my $thinning_fuzziness = .3;

    # Parse the thinning config
    my %thinning_conf;
    if($thinning){
        foreach my $t (split(/,/, $thinning)){
            my ($age, $gap) = split(/:/, $t);
            $age =~ s/^\s+|\s+$//g;
            $gap =~ s/^\s+|\s+$//g;
            die "ERROR: age missing from thinning config\n" unless defined $age;
            die "ERROR: gap missing from thinning config\n" unless $gap;
            $thinning_conf{$age} = $gap;
        }
        die "ERROR: no entries in thinning config\n" unless scalar(%thinning_conf);
        $thinning_conf{0} = 0 unless defined $thinning_conf{0};
    }

    # select backups to delete
    my (%backups, %backup_objs, @backups_to_delete) = ();

    # Separate backups by source
    # {METANAME}
    foreach my $b ($self->backups) {
        my $backup_name = $b->filename;
        if ($backup_name =~ /\.(stopped|with-error)($|\.)/){
            $b->set_incomplete(1); # mark the backup object
        }
        if ($backup_name =~ /^(.*?)-[^-]+-\d+($|\.)/) {
            $backups{$1} ||= [];
            push @{ $backups{$1} }, $backup_name;
            $backup_objs{$1} ||= [];
            push @{ $backup_objs{$1} }, $b;
        }
        else {
            die "Unexpected backup name format: '$backup_name'";
        }
    }

    # Enable to debug the thinning logic
    if(0){
        %backups = ( $self->{name} => [] );
        %backup_objs = ( $self->{name} => [] );
        my $start = time()-47*60*60;
        for(my $i=0;$i<120;$i++){
            my $name = 'backup'.$i;
            my $bo = Brackup::TargetBackupStatInfo->new($self, $name,
                                                        time => $start - ($i*60*60*12),
                                                        size => 5);
            if(int(rand(3))==0){ $bo->set_incomplete(1); }
            push @{ $backups{$self->{name}} }, $name;
            push @{ $backup_objs{$self->{name}} }, $bo;
        }
    }

    foreach my $source (keys %backups) {
        next if $opt{source} && $source ne $opt{source};

        if($thinning){
            my $prevtime;
            my $debug;
            my @backups_chron = sort { $a->time <=> $b->time } @{ $backup_objs{$source} };

            # Never delete the latest complete backup, and ignore subsequent incomplete ones.
            # The ages of backups are relative to the latest complete backup.
            my $latest_bo; # The latest complete backup
            my @latest_incomplete_bobjs; # Subequent incomplete backups
            while(1){
                $latest_bo = pop(@backups_chron);
                if($latest_bo->incomplete) {
                    unshift @latest_incomplete_bobjs, $latest_bo;
                }
                else {
                    last;
                }
            }

            # Add the reference point to the backups
            foreach my $bo (@backups_chron){ $bo->set_now( $latest_bo->time ); }
            $latest_bo->set_now( $latest_bo->time );
            foreach my $bo (@latest_incomplete_bobjs){ $bo->set_now( $latest_bo->time ); }

            # Return the desired gap for the backup
            my $desiredgap_from_age = sub {
                my $ageindays = shift;

                my $desiredgap = 0;
                ## sort in itself appears to sort the keys as strings
                foreach my $conf_age (sort { $a <=> $b } keys %thinning_conf){
                    $desiredgap = $thinning_conf{$conf_age} if $ageindays >= $conf_age;
                }

                return $desiredgap;
            };

            my $logline = sub {
                return unless $opt{verbose};
                my $bo = shift;
                my $dodelete = shift; # 'del' or any other value
                my $reason = shift;
                warn 'Thinning:'
                . ' cmpl:' . ($bo->incomplete ? 'n' : 'y')
                . ' age:' . $bo->ageindays
                . ' des:' . &$desiredgap_from_age( $bo->ageindays )
                . ' gap:' . ((defined $prevtime) ? sprintf('%.2f', ($bo->time - $prevtime) / 60 / 60 / 24) : 'n/a')
                . ($dodelete eq 'del' ? ' * DELETE' : ' - keep  ')
                . ' (' . $reason. ')'
                . ' - ' . $bo->filename
                . "\n";
            };

            my $deleteme = sub {
                my $bo = shift;
                my $reason = shift;
                &$logline($bo, 'del', $reason);
                push @backups_to_delete, $bo->filename;
            };

            my $keepme = sub {
                my $bo = shift;
                my $reason = shift;
                &$logline($bo, 'keep', $reason);
                $prevtime = $bo->time;
            };

            for(my $i=0; $i<scalar(@backups_chron); $i++){

                # We loop through the points in forward chronological order, so age is decreasing!

                my $bo = $backups_chron[$i];

                my $desiredgap = &$desiredgap_from_age( $bo->ageindays );

                # If according to the configuration, we should delete all backups that are this old:
                if( $desiredgap eq 'delete' ){
                    &$deleteme($bo, 'desiredgap==delete');
                    next;
                }

                # If desiredgap is 0, we necessarily keep the backup
                if( $desiredgap == 0 ){
                    &$keepme($bo, 'desiredgap==0');
                    next;
                }

                # If there is no previous backup, we pretend that its time is our time - desired gap
                # so that the fuzziness logic would apply to the very first backup as well
                $prevtime = $bo->time - $desiredgap * 60 * 60 * 24 unless defined $prevtime;

                # We can calculate the gap now
                my $gapindays = ($bo->time - $prevtime) / 60 / 60 / 24;

                # If the gap is smaller than the desired gap - fuzziness, then we delete
                if( $gapindays < $desiredgap * (1 - $thinning_fuzziness ) ){
                    &$deleteme($bo, 'gap too small');
                    next;
                }

                # If this is a complete backup, and the gap is large enough, we are sure to keep it
                if( (! $bo->incomplete ) && $gapindays >= $desiredgap ){
                    &$keepme($bo, 'is complete');
                    next;
                }

                # Do a look-ahead to find a complete backup close to the desired gap
                my $foundcomplete = undef;
                my $gapdiff = $desiredgap * 2; # a large number
                for(my $j=$i; $j<scalar(@backups_chron); $j++){
                    my $lbo = $backups_chron[$j];
                    warn "  Look-ahead: " . $lbo->filename . "\n" if $opt{verbose} && $opt{verbose} > 1;

                    # Stop if we get to a new thinning section
                    if(&$desiredgap_from_age( $lbo->ageindays ) != $desiredgap){
                        warn "    next section reached\n" if $opt{verbose} && $opt{verbose} > 1;
                        last;
                    }

                    my $lgapindays = ($lbo->time - $prevtime) / 60 / 60 / 24;

                    # Stop if we have looked ahead more than the fuzziness
                    if($lgapindays > $desiredgap * (1 + $thinning_fuzziness)){
                        warn "     fuzziness limit reached '$lgapindays'\n" if $opt{verbose} && $opt{verbose} > 1;
                        last;
                    }

                    # We're not interested in incomplete backups
                    if($lbo->incomplete){
                        warn "    incomplete\n" if $opt{verbose} && $opt{verbose} > 1;
                        next;
                    }

                    my $lgapdiff = abs($lgapindays - $desiredgap);
                    warn "    diff: '$lgapdiff'\n" if $opt{verbose} && $opt{verbose} > 1;
                    if($gapdiff > $lgapdiff){
                        warn "    SAVING\n" if $opt{verbose} && $opt{verbose} > 1;
                        $foundcomplete = $j;
                        $gapdiff = $lgapdiff;
                    }
                }

                # No complete backup near us; fall back to old logic
                unless(defined $foundcomplete){
                   if( $gapindays >= $desiredgap ){
                       &$keepme($bo, 'no complete near');
                   }
                   else {
                       &$deleteme($bo, 'no complete near');
                   }
                   next;
                }

                # We found a complete backup near
                # Delete all backups up to the one found
                for(my $j=$i; $j<$foundcomplete; $j++){
                    &$deleteme($backups_chron[$j], 'fuzzy search');
                }
                # Keep the backup found
                $i = $foundcomplete; # Jump ahead
                &$keepme($backups_chron[$foundcomplete], 'fuzzy search');

            }

            if($opt{verbose}) {
                &$keepme($latest_bo, 'last complete');
                foreach my $bo (@latest_incomplete_bobjs){
                    &$keepme($bo, 'recent incomplete');
                }
            }

        }
        else{
            my @b = reverse sort @{ $backups{$source} };
            push @backups_to_delete, splice(@b, ($keep_backups > $#b+1) ? $#b+1 : $keep_backups);
        }
    }

    warn ($opt{dryrun} ? "Would prune:\n" : "Pruned:\n") if $opt{verbose} || $opt{dryrun};
    foreach my $backup_name (@backups_to_delete) {
        warn "  $backup_name\n" if $opt{verbose} || $opt{dryrun};
        $self->delete_backup_and_local_metafile($backup_name, $opt{meta_dir}) unless $opt{dryrun};
    }
    return scalar @backups_to_delete;
}

# Returns the name of the downloaded and optionally decrypted file
# AND an object that needs to remain in scope until the file is used!
sub get_and_decrypt_backup {
    my $self = shift;
    my $name = shift;
    my $opt = shift;

    my $tempfile = +(tempfile())[1];
    $self->get_backup($name, $tempfile) || die "Couldn't load backup from target " . $name;

    # We CANNOT let this go out of scope as then the decrypted file will get deleted!
    my $fobj = Brackup::DecryptedFile->new($tempfile, $opt);

    return ( ($fobj->name || $tempfile), $fobj );
}

sub item_in_backup_progress {
    my $self = shift;
    my $slot = shift;
    return ( ($slot > 0) ? ("\n" x $slot) : "", "\x1B[K\n" . "\x1B[" . ($slot+1) . "A" );
}

sub item_in_backup {
    my $self = shift;
    my $callback = shift;
    my $opt = shift;
    my $backup = shift;
    my $filename = shift;
    my $localfile = shift;
    my $count = shift;
    my $backups = shift;
    my $slot = shift;

    my ($head, $tail, $format, $log);
    if($opt->{progress}){
        ($head, $tail) = $self->item_in_backup_progress($slot);
        $format = "[%3d%%] (%8d/%8d) ";
        $log = $head . sprintf "    * Backup [%3d/%3d] (%d) ", $count, $backups, $slot;
    }
         
    my $parser = Brackup::Metafile->open($localfile);
         
    my $size = (-s $localfile);
         
    my $t_log = time;
    
    my $it = $parser->readline;
    my $filecount = &$callback($it, 1, $localfile);
    
    my $fileno = 0;
    for(my $fileno = 0; my $it = $parser->readline; $fileno++) {
        if( $opt->{progress} && (time - $t_log) >= 1 ) {
            if($filecount) {
                print STDERR $log . sprintf($format, $fileno / $filecount * 100, $fileno, $filecount) . $filename . $tail;
            }
            else {
                print STDERR $log . sprintf($format, tell($parser->{fh}) / $size * 100, $parser->{linenum}) . $filename . $tail;
            }
            $t_log = time;
        }
        
        &$callback($it, 0, $localfile);
    }
    
    print STDERR $log . sprintf($format, '100', $fileno, $filecount) . "$filename done." . $tail if $opt->{progress};
    
    close $parser->{fh};
}

# Opens all metafiles and loops through their sections
# &$callback( $meta_file_item, $is_header, $backup_name ) is called for each item in each backup
# Returns the number of metafiles found
sub loop_items_in_backups {
    my $self = shift;
    my $callback = shift;
    my $opt = shift;
    ##
    # meta_dir -- if specified, attempt to read local metafiles
    # progress
    # no_gpg
    # threads -- number of threads to use, or 0 if threading shouldn't be used at all

    # Enable autoflush for smoother progress logging
    select(STDERR); $|=1; select(STDOUT); $|=1;
    
    my @backups = $self->backups;
    
    my $threads = {}; # Thread ID => Slot Number
    my $slots = []; # Slot Number => Thread ID
    my $slot = 0;
    
    foreach my $i (0 .. $#backups) {
         
        while( $self->{threads} && (keys %$threads >= $self->{threads}) ) {
            # Loop through all the threads
            if(my @joinable = threads->list(&threads::joinable)) {
                foreach my $thr (@joinable) {
                    undef $slots->[ $threads->{$thr->tid}->{slot} ];
                    unless( $thr->join() ) {
                        # Position cursor below the progress meter lines
                        print STDERR [$self->item_in_backup_progress($self->{threads})]->[0] if $opt->{progress};
                        my $error = $thr->error();
                        die "Aborting on failure to read metafile '" . $threads->{$thr->tid}->{localfile} . "'" 
                            . ", error '$error'"
                            ;
                        }
                    delete $threads->{$thr->tid};
                }
            }
            else {
                sleep 0.25;
            }
        }
         
        # Find spare thread slot
        for($slot = 0; $slots->[$slot]; $slot++) {;}       
        my ($head, $tail) = $self->item_in_backup_progress($slot);
         
        my $backup = $backups[$i];
         
        my $localfile;
        my $parser;
        # Try local file
        if( $opt->{meta_dir} ){
            $localfile = File::Spec->catfile($opt->{meta_dir}, $backup->filename);
        }
         
        # If not available:
        my $fobj;
        unless(-e $localfile) {
            print STDERR $head . sprintf("    * Backup [%3d/%3d] (%d) [    ] (        /        ) %s (downloading from target)", $i+1, scalar(@backups), $slot, $backup->filename) . $tail
                if $opt->{progress};
              
            ($localfile, $fobj) = $self->get_and_decrypt_backup($backup->filename, $opt);
        }
        
        print STDERR $head . sprintf("    * Backup [%3d/%3d] (%d) [    ] (        /        ) %s", $i+1, scalar(@backups), $slot, $backup->filename) . $tail
            if $opt->{progress};

        if($self->{threads}) {
            my $thread = threads->create(
                sub {
                    eval {
                        $self->item_in_backup( $callback, $opt, $backup, $backup->filename, $localfile, $i+1, scalar(@backups), $slot );
                        return 1;
                    } || do {
                        # Inhibit the "Thread XX terminated abnormally" message
                        close STDERR;
                        # Propagate the exception
                        die $@;
                    };
                    return 1;
                }
            );
         
            $threads->{$thread->tid} = { 'slot' => $slot, 'fobj' => $fobj, 'localfile' => $localfile };
            $slots->[$slot] = $thread->tid;
        }
        else {
            $self->item_in_backup( $callback, $opt, $backup, $backup->filename, $localfile, $i+1, scalar(@backups), $slot );
        }
    }

    if($self->{threads}) {
        # Loop through all the threads
        foreach my $thr (threads->list()) {
            unless( $thr->join() ) {
                # Position cursor below the progress meter lines
                print STDERR [$self->item_in_backup_progress($self->{threads})]->[0] if $opt->{progress};
                die "Aborting on failure to read metafile '" . $threads->{$thr->tid}->{localfile} . "'" 
                   . " , error '" . $thr->error() . "'"
                   ;
            }
        }
    }
    
    print STDERR ("\n" x (@$slots)) if $opt->{progress};
    warn "    * Finished loading and analysing metafiles.\n";
    
    return scalar(@backups);
}

sub fsck {
    my $self = shift;
    my $opts = shift;
    ##
    # meta_dir
    # verbose
    # dryrun
    # threads -- number of threads to use
    # skip_gc
    # force_gc

    my $label_dryrun = $opts->{dryrun} ? '(DRY RUN)' : '';
    warn "* Fsck starts $label_dryrun\n";

    my $step;
    my $gpg_rec;

    # STAGE I
    # CURRENT DATA INTEGRITY
    # Compare chunks referenced in metafiles to chunks in the target.
    # Any error here relates to the integrity of the backed up data.

    # A temporary inventory (%INV) is constructed using all the valid chunks;
    # in other words, %INV will be the "intersection" of the chunks in the metafiles and those in the target.
    # A chunk is valid if it not only exists in the target, but also passes some checks,
    # for example, its size in the target matches the size stored in the metafile.

    # %USEDCHUNKS will also store the valid chunks, which is then used for garbage collection.
    # By default, garbage collection will be skipped if any errors are encountered here,
    # which should prevent deleting invalid chunks automatically.

    warn "* Checking current target data integrity\n";
    warn "  * Enumerating chunks on the target\n";

    # Get a list of chunks on the target with their size
    my $CHUNKS = $self->chunks_with_length;
    
    warn "    * Enumerated " . scalar(keys %$CHUNKS) . " target chunks\n";

    # Loop through all backup (meta) files and recreate the inventory into this hash
    my %INV :shared = ();
    my %META_ERRORS :shared = ();

    # Store names of used chunks here
    my %USEDCHUNKS :shared = ();

    my @labels = ('p_offset', 'p_length', 'range_or_s_length', 's_digest', 'p_digest');
    
    warn "  * Enumerating metafile chunks and checking against target\n";
    warn "    * Wherever possible, locally stored metafiles will be used\n" if $opts->{meta_dir};
    my $num_metafiles = $self->loop_items_in_backups (sub {

        my $item = shift;
        my $is_header = shift;
        my $backupname = shift;

        # Parse the metafile

        if($is_header){
            $gpg_rec = $item->{'GPG-Recipient'};
            return $item->{'FileCount'};
        }

        return unless $item->{Chunks}; # skip non-file entries

        my $filedigest = $item->{Digest} || die "Cannot find file digest in an item in metafile '$backupname'";
        my $singlechunk;
        my $no_filechunks = 0;

        foreach my $filechunk (split(/\s+/, $item->{Chunks})){
            $no_filechunks++;

            # {METASYNTAX} (search for this label to see where else this syntax is used) -- see Brackup::StoredChunk::to_meta

            my %chunkdata = ();
            @chunkdata{@labels} = split(/;/, $filechunk);

            if($chunkdata{range_or_s_length} =~ /^(\d+)-(\d+)$/){
                $chunkdata{s_range} = $chunkdata{range_or_s_length};

                # The s_length is the full length of the composite chunk, but how are we supposed to know this?
                # We will use the length reported by the target for now.
                # There is some sanity checking below.
                $chunkdata{s_length} = $CHUNKS->{ $chunkdata{s_digest} };
                $chunkdata{s_range_from} = $1;
                $chunkdata{s_range_to} = $2;
            }else{
                $chunkdata{s_length} = $chunkdata{range_or_s_length};
            }

            # An explicit p_digest is only added if encrypted AND the file contains one chunk only
            # BUT! With composite chunks, the p_digest is STILL the file digest! {MISSINGPDIGEST}
            unless($chunkdata{p_digest}){
                if($gpg_rec || $chunkdata{s_range}){
                    $singlechunk = 1; # used to assert later
                    $chunkdata{p_digest} = $filedigest;
                }
                else{ # Without encryption, the raw digest is the stored digest
                    $chunkdata{p_digest} = $chunkdata{s_digest};
                }
            }

            # Check the chunk on the target
            my $error;
            if(!exists $CHUNKS->{ $chunkdata{s_digest} }){
                # MOT - Missing On Target
                $error = ['MOT', $backupname, 'missing on target', "Line in metafile: '$filechunk'"];
            }
            else {
                my $size_on_target = $CHUNKS->{ $chunkdata{s_digest} };
                if($chunkdata{s_range}){
                    unless($size_on_target >= $chunkdata{s_range_to}){
                    # TSOT - Too Small On Target
                    $error = ['TSOT', $backupname, 'too small on target to contain range', "Line in metafile: '$filechunk'", "Size in metafile: '$chunkdata{s_length}'", "Size on target: '$size_on_target'"];
                    }
                }
                else{
                    unless($size_on_target == $chunkdata{s_length}){
                        # WSOT - Wrong Size On Target
                        $error = ['WSOT', $backupname, 'has wrong size on the target', "Line in metafile: '$filechunk'", "Size in metafile: '$chunkdata{s_length}'\n - Size on target: '$size_on_target'"];
                    }
                }
            }

            if($error) {
                # FIXME: Potential race condition - locking needed?
                if( ! exists $META_ERRORS{$chunkdata{s_digest}} ) {
                    $META_ERRORS{$chunkdata{s_digest}} = shared_clone([$error]);
                }
                else {
                    push( @{$META_ERRORS{$chunkdata{s_digest}}}, shared_clone($error) );
                }
                next;
            }
              
            # This target chunk is referenced, exists, passes validity checks, and so should be kept when garbage collecting
            $USEDCHUNKS{ $chunkdata{s_digest} }++;

            # Create inventory entry in %INV
            # {INVSYNTAX} (search for this label to see where else this syntax is used)

            my $db_key = $chunkdata{p_digest};
            $db_key .= $gpg_rec ? ';to=' . $gpg_rec : ';raw';

            my $db_value = $chunkdata{s_digest} . ' ' . $chunkdata{s_length};
            $db_value .= ' ' . $chunkdata{s_range} if $chunkdata{s_range};

            # FIXME: Report or assert if multiple values exist for this key
            $INV{$db_key} = $db_value;
        }

        if($singlechunk){
            die "ASSERT: Expected a one-chunk file based on metafile '$backupname'" unless $no_filechunks == 1;
        }

    }, $opts);
    
    warn "    * Analysed $num_metafiles metafiles\n";

    my $num_error_chunks;
    my $num_chunk_errors;
    my $meta_error_detail = '';
    while( my($dig, $errors) = each %META_ERRORS) {
        $num_error_chunks++;
        $num_chunk_errors += @$errors;
        $meta_error_detail .= "      * Chunk '$dig'\n" if $opts->{verbose};
        if($opts->{verbose} >= 2) {
            foreach my $error (@$errors) {
                $meta_error_detail .= "        * Referred to in metafile '$$error[1]' - $$error[2]\n";
                for(my $i=3; $i < scalar(@$error); $i++) {
                    $meta_error_detail .= "          - " . $$error[$i] . "\n";
                }
            }
        }       
    }

    if($num_error_chunks) {
        warn "    * $num_chunk_errors errors found with $num_error_chunks chunks.\n" . $meta_error_detail;
        warn "    * These issues cannot be fixed, so consider deleting the affected backups\n";
    }   
    else {
        warn "    * Found no errors: all metafile chunks found on target\n";
        warn "  * Metafiles are consistent with target\n";
    }
    
    # Abort if we haven't managed to collect any data because we haven't found any metafiles.
    # This can be due to a misconfiguration; for example, the naming prefix is wrong.
    if( (!$num_metafiles) && (!$opts->{ignore_no_meta}) ){
        die "Aborting as no metafiles were found. Use --ignore-no-metafiles to ignore this and continue.\n";
    }

    # STAGE II
    # FUTURE DATA INTEGRITY
    # We check the inventory database. Any errors here affect the integrity of future backups.
    # Currently, the inventory is compared to the temporary inventory constructed during stage one in both directions.
    # Missing entries are added, and superfluous entries are (or may optionally be) deleted.

    warn "* Checking local inventory to ensure future target integrity\n";
    warn "  * Comparing local inventory with valid metafile chunks\n";

    my $label_curval = 'Description in inventory:';
    my $label_bkpval = 'Description from target :';

    my %INV_ERRORS = ('MM' => 0, 'MOT' => 0, 'TSOT' => 0, 'WSOT' => 0, 'URM1' => 0, 'URM2' => 0, 'MIE' => 0);
    
    my $inv_error_detail = '';
    my $inv_error_detail_urm1 = '';
    
    my %INV_UNUSED_TCHUNKS;
    while (my ($key, $curval) = $self->inventory_db->each) {
        my $bkpval = $INV{$key};
         
        # If referenced by metafiles, valid and on target
        if($bkpval) {

            if($curval eq $bkpval) {
                # %INV is destroyed in this process, when the local inventory record for $key is consistent.
                # Only inconsistent entries will remain in %INV, which will be fixed up in the next stage.
                delete $INV{$key};
            }
            else {
                  
                $INV_ERRORS{MM}++;
                $inv_error_detail .=  "    * Inventory source chunk '$key' value doesn't match expected value - updating inventory $label_dryrun\n" if $opts->{verbose};
                $inv_error_detail .=  "      - $label_curval '$curval'\n - $label_bkpval '$bkpval'\n" if $opts->{verbose} >= 2;
                  
                $self->inventory_db->set($key, $bkpval) unless $opts->{dryrun};
                  
                # %INV is destroyed in this process, when the local inventory record for $key is consistent.
                # Only inconsistent entries will remain in %INV, which will be fixed up in the next stage.
                delete $INV{$key};
            }
        }
        else {
            # {INVSYNTAX}
            my $curval_s_digest = [split(' ',$curval)]->[0];
            if( my $code = $META_ERRORS{$curval_s_digest} ) {
                $INV_ERRORS{$code}++;
                $inv_error_detail .=  "    * Inventory source chunk '$key' referenced by metafiles but target chunk missing/invalid on target (code '$code') - deleting to allow re-upload $label_dryrun\n" if $opts->{verbose};
                $inv_error_detail .=  "      - $label_curval '$curval'\n" if $opts->{verbose} >= 2;
                $inv_error_detail .=  "      - $label_curval s_digest '$curval_s_digest', META_ERRORS{s_digest} = '$META_ERRORS{$curval_s_digest}'\n" if $opts->{verbose};
                 
                $self->inventory_db->delete($key) unless $opts->{dryrun};
            }
            else {
                if( $CHUNKS->{$curval_s_digest} ) {
                    # URM1 = Unreferenced by Metafile case 1 (stored chunk digest exists on target)
                    $INV_ERRORS{URM1}++;
                     
                    # Target chunk may be either unused, or may be a composite chunk referenced elsewhere in the metafiles by some other source chunk.
                     
                    # FIXME:
                    # Deleting these items from inventory, and subsequently garbage collecting the corresponding target chunks
                    # is an optimisation that legitimately may or may not be performed without affecting target integrity,
                    # assuming the inventory items adequately validate against the target (by size, range, etc).
                    #
                    $inv_error_detail_urm1 .=  "    * Inventory source chunk '$key' not referenced by metafiles but target chunk still present" .
                      ($opts->{skip_gc} ? ' - ignoring' : " - deleting $label_dryrun\n") if $opts->{verbose};
                    $inv_error_detail_urm1 .=  "      - $label_curval '$curval'\n" if $opts->{verbose} >= 2;
                    $inv_error_detail_urm1 .=  "      - $label_curval s_digest '$curval_s_digest', USEDCHUNKS=" . exists($USEDCHUNKS{$curval_s_digest}) . "\n" if $opts->{verbose} >= 2;
                    
                    # Keep count of unused target chunks - should be <= count of orphans found later
                    # (there may exist orphan chunks on the target that are missing from the inventory)
                    
                    # FIXME: Is the condition necessary? Doesn't exists($USEDCHUNKS{$curval_s_digest}) => $INV{$key} => $bkpval true?
                    $INV_UNUSED_TCHUNKS{$curval_s_digest}++ if !exists($USEDCHUNKS{$curval_s_digest});
                        
                    $self->inventory_db->delete($key) unless $opts->{dryrun} || $opts->{skip_gc};
                }
                else {
                    # URM2 = Unreferenced by Metafile case 2
                    $INV_ERRORS{URM2}++;
                    $inv_error_detail .=  "    * Inventory source chunk '$key' neither referenced by metafiles nor found on target - deleting $label_dryrun\n" if $opts->{verbose};
                    $inv_error_detail .=  "      - $label_curval '$curval'\n" if $opts->{verbose} >= 2;
                    
                    $self->inventory_db->delete($key) unless $opts->{dryrun};
                }
            }
        }
    }

     warn sprintf "    * " . ($opts->{skip_gc} ? 'Ignoring' : 'Deleting') . " %d inventory source chunk key(s) not referenced by metafiles but for which " . scalar(keys %INV_UNUSED_TCHUNKS) . " target chunks still exist $label_dryrun\n", $INV_ERRORS{URM1} if $INV_ERRORS{URM1};
     warn $inv_error_detail_urm1 if $inv_error_detail_urm1;
    
     my $inv_error_keys = 0;
     $inv_error_keys += $INV_ERRORS{$_} foreach qw( MM MOT TSOT WSOT URM2 );
     if( $inv_error_keys ) {
         warn sprintf "    * Deleting %d inventory source chunk key(s) for which target chunk isn't as expected $label_dryrun\n", $INV_ERRORS{MM} if $INV_ERRORS{MM};
         warn sprintf "    * Deleting %d inventory source chunk key(s) referenced by metafiles but missing on target $label_dryrun\n", $INV_ERRORS{MOT} if $INV_ERRORS{MOT};
         warn sprintf "    * Deleting %d inventory source chunk key(s) referenced by metafiles but too small on target $label_dryrun\n", $INV_ERRORS{TSOT} if $INV_ERRORS{TSOT};
         warn sprintf "    * Deleting %d inventory source chunk key(s) referenced by metafiles but wrong size on target $label_dryrun\n", $INV_ERRORS{WSOT} if $INV_ERRORS{WSOT};
         warn sprintf "    * Deleting %d inventory source chunk key(s) neither referenced by metafiles nor found on target $label_dryrun\n", $INV_ERRORS{URM2} if $INV_ERRORS{URM2};
         warn $inv_error_detail if $inv_error_detail;
     }
     else {
         warn "    * Inventory has no superfluous or conflicting entries (as compared to metafiles and target)\n";
     }
    
     warn "  * Checking inventory for missing metafile chunk entries\n";
     # MIE - Missing Inventory Entry
     # Add to the inventory any valid items found in the metafiles that were not found in the inventory
     while( my($key, $bkpval) = each %INV ) {
        warn "    * Metafile chunk '$key' missing in inventory, adding. $label_dryrun\n" if $opts->{verbose};
        warn "      - $label_bkpval '$bkpval'\n" if $opts->{verbose} && $opts->{verbose} >= 2;
        $INV_ERRORS{MIE}++;
        unless($opts->{dryrun}){
            $self->inventory_db->set($key, $bkpval);
        }
    }
    
    if($INV_ERRORS{MIE}) {
        warn "    * Added $INV_ERRORS{MIE} metafile chunk(s) to the local inventory $label_dryrun\n";
    }
    else {
        warn "    * Found no metafile chunks missing from local inventory\n";
    }
    
    if( $inv_error_keys + $INV_ERRORS{MIE} == 0) {
        warn "  * Inventory consistent!\n";
    }
    else {
        if($opts->{dryrun}) {
            warn "  * Inventory not yet consistent\n";
        }
        else {
            warn "  * Issues found but fixed\n";
            warn "  * Inventory consistent!\n";
        }
    }
    
    # STAGE III
    # GARBAGE COLLECTION
    # We use %USEDCHUNKS generated during stage one to delete chunks
    # from both the inventory and the target that are not referenced by
    # any of the metafiles.

    warn "* Garbage collection\n";
    my $orphans;
    while(1) {

        if( $opts->{skip_gc} ) {
            warn "  - Skipped (--skip-gc was used)\n";
            last;
        }

        # If errors have been encountered in step 1 - in other words chunks in the metafile
        # could not be found or weren't found to be valid on the target - then it would be rash
        # to automatically garbage collect unused target chunks particularly when this scenario
        # might arise from running fsck for a target against the wrong set of metafiles.
        if( $num_error_chunks && ! $opts->{force_gc} ) {
            warn "  * It is unsafe to garbage collect as some metafile target chunks were invalid or not found on the target. Use --force-gc to override this.\n";
            last;
        }

        # Get orphaned chunks
        # WARNING %$CHUNKS is destroyed in this process
        while( my($k, $v) = each %USEDCHUNKS ) {
            die "ASSERT: chunk marked as used should exist" unless exists $CHUNKS->{$k};
            delete $CHUNKS->{$k};
        }

        $orphans = scalar(keys %$CHUNKS);

        warn "  * Deleting $orphans orphan chunks $label_dryrun\n";
         
        # No orphaned chunks?
        last unless $orphans;

        # On very verbose, print chunks to be deleted
        if( $opts->{verbose} ){
            while( my($k, $v) = each %$CHUNKS ) {
                warn "    * $k Size: $CHUNKS->{$k}\n";
            }
        }

        # Dry run
        last if $opts->{dryrun};

        # Confirmation
        my $confirm = 'y';
        unless($opts->{automatic}){
            printf "  * Run gc, removing %d orphaned chunks? [y/N] ", $orphans;
            $confirm = <>;
        }
        last unless (lc substr($confirm,0,1) eq 'y');

        # (1) delete orphaned chunks from inventory and THEN from the target
        my $inventory_db = $self->inventory_db;
        while (my ($k, $v) = $inventory_db->each) {
            $v =~ s/ .*$//;         # strip value back to hash
            if(exists $CHUNKS->{$v}) {
                    
                # Unnecessary, as relevant inventory entries will already have been deleted in previous stage
                # but for avoidance of doubt, delete from local inventory again.
                $inventory_db->delete($k); 
                
                # Delete (or schedule delete) from the target
                $self->delete_chunk_multi($v);
                    
                # Eliminate records from %$CHUNKS as we go, for next stage.
                delete $CHUNKS->{$v};
            }
        }

        # (2) delete chunks not found in the inventory
        while( my($k, $v) = each %$CHUNKS ) {
            $self->delete_chunk_multi($k);
        }

        last;
    } # end while(1)

    # Execute any queued deletes.
    $self->delete_chunks_multi();

    # Calculate a suitable return code (1=OK; see brackup-target for process return code)
    my $ret =
      !$num_error_chunks # Fail: These errors cannot be fixed
      && (!$inv_error_keys || !$opts->{dryrun}) # Fail if these issues were not fixed
      ;
    
     warn "* Fsck finished with code " . ($ret ? 'OK' : 'FAIL') . "\n";
     
     return $ret;
}

1;

__END__

=head1 NAME

Brackup::Target - describes the destination for a backup

=head1 EXAMPLE

In your ~/.brackup.conf file:

  [TARGET:amazon]
  type = Amazon
  aws_access_key_id  = ...
  aws_secret_access_key =  ....

=head1 GENERAL CONFIG OPTIONS

=over

=item B<type>

The driver for this target type.  The type B<Foo> corresponds to the Perl module
Brackup::Target::B<Foo>.

The set of targets (and the valid options for type) currently distributed with the
Brackup core are:

B<Filesystem> -- see L<Brackup::Target::Filesystem> for configuration details

B<Ftp> -- see L<Brackup::Target::Ftp> for configuration details

B<Sftp> -- see L<Brackup::Target::Sftp> for configuration details

B<Amazon> -- see L<Brackup::Target::Amazon> for configuration details

B<CloudFiles> -- see L<Brackup::Target::CloudFiles> for configuration details

B<Riak> -- see L<Brackup::Target::Riak> for configuration details

=item B<keep_backups>

The default number of recent backups to keep when running I<brackup-target prune>.

=item B<thinning>

Applies to I<brackup-target prune> and specifies how backups should be thinned.
Incompatible with I<keep_backups>.

<format> := <thinpoint> "," <thinpoint>+
<thinpoint> := <age_in_days> ":" ( <desired_gap_in_days> | "delete" )

For example, I<thinning = 7:2,14:4,31:delete> means that after 7 days as we go back in time,
the gap between retained backups should be at least 2 days; after 14 days, it should be at least 4 days;
and after 31 days, no backups should be retained.

Thinning applies to each source individually.

=item B<inventorydb_file>

The location of the L<Brackup::InventoryDatabase> inventory database file for
this target e.g.

  [TARGET:amazon]
  type = Amazon
  aws_access_key_id  = ...
  aws_secret_access_key =  ...
  inventorydb_file = /home/bradfitz/.amazon-already-has-these-chunks.db

Only required if you wish to change this from the default, which is
".brackup-target-TARGETNAME.invdb" in your home directory.

=item B<inventorydb_type>

Dictionary type to use for the inventory database. The dictionary type B<Bar>
corresponds to the perl module Brackup::Dict::B<Bar>.

The default inventorydb_type is B<SQLite>. See L<Brackup::InventoryDatabase> for
more.

=item B<inherit>

The name of another Brackup::Target section to inherit from i.e. to use
for any parameters that are not already defined in the current section e.g.:

  [TARGET:ftp_defaults]
  type = Ftp
  ftp_host = myserver
  ftp_user = myusername
  ftp_password = mypassword

  [TARGET:ftp_home]
  inherit = ftp_defaults
  path = home

  [TARGET:ftp_images]
  inherit = ftp_defaults
  path = images

=item B<daemons>

Specifies the maximum number of child processes used to store chunks in parallel.

=item B<threads>

Specifies the maximum number of threads used to read metafiles by I<fsck>.

=item B<gpg_daemons>

Specifies the maximum number of child processes used to encrypt chunks in parallel.

=item B<local_meta_dir>

Should be a directory; optional.
If specified, I<brackup> saves the metafile in this directory,
and I<brackup-target fsck> will attempt to load the metafiles from this directory.
(It falls back to loading the metafiles from the target in case of failure,
and the list of metafiles (i.e. backups) is still loaded from the target.)
This is useful if the metafiles on the target are encrypted, as if all metafiles
are available locally, then these tasks can run without having to enter
a passphrase for GPG for decryption.

Also, I<brackup-target prune> and I<delete_backup> delete the local metafile as well,
and I<get_backup> and I<get_backups> write the backup file to this directory.

See also the --meta-dir command-line option of I<brackup> and I<brackup-target>.

=back

=head1 SEE ALSO

L<Brackup>

L<Brackup::InventoryDatabase>

=cut

# vim:sw=4:et

