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
                                        $opts->{create_new_inv});

    $self->{daemons} = $confsec->value("daemons") || '0';
    $self->{gpg_daemons} = $confsec->value("gpg_daemons") || '0';
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

# returns a list of names of all chunks
sub chunks {
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

# removes old metafiles from this target
sub prune {
    my ($self, %opt) = @_;

    my $keep_backups = defined $opt{keep_backups} ? $opt{keep_backups} : $self->{keep_backups};
    my $thinning = $self->{thinning};
    die "ERROR: keep_backups or thinning option not set\n" if ! defined $keep_backups and ! defined $thinning;
    die "ERROR: keep_backups option must be at least 1 if no source is specified\n"
        if( ! $opt{source} and ! $thinning and $keep_backups < 1 );

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
    foreach my $b ($self->backups) {
        my $backup_name = $b->filename;
        if ($backup_name =~ /^(.+)-\d+(\.brackup)?$/) {
            $backups{$1} ||= [];
            push @{ $backups{$1} }, $backup_name;
            $backup_objs{$1} ||= [];
            push @{ $backup_objs{$1} }, $b;
        }
        else {
            warn "Unexpected backup name format: '$backup_name'";
        }
    }

    foreach my $source (keys %backups) {
        next if $opt{source} && $source ne $opt{source};

        if($thinning){
            my $prevage;
            my $debug;
            my @backups_chron = sort { $a->time <=> $b->time } @{ $backup_objs{$source} };

            # Never delete the latest backup
            pop(@backups_chron);

            foreach my $bo (@backups_chron) {
                ## We loop through the points in forward chronological order, so age is decreasing!
                my $bn = $bo->filename;
                my $ageindays = int( $bo->time / 60 / 60 / 24 + .5 );
                my $gapindays = int( ($prevage - $bo->time) / 60 / 60 / 24 + .5 ) if defined $prevage;
                my $desiredgap;

                foreach my $conf_age (reverse sort keys %thinning_conf){
                    $desiredgap = $thinning_conf{$conf_age} if $ageindays >= $conf_age;
                }

                $debug =  "Thinning: $bn age:$ageindays gap:$gapindays des:$desiredgap" if $opt{verbose};

                if( (defined $prevage) && ( $desiredgap eq 'delete' || $gapindays < $desiredgap ) ) {
                    push @backups_to_delete, $bn;
                    warn $debug . " * DELETE\n" if $opt{verbose};
                }
                else {
                    $prevage = $bo->time;
                    warn $debug . " - keep\n" if $opt{verbose};
                }
            }
        }
        else{
            my @b = reverse sort @{ $backups{$source} };
            push @backups_to_delete, splice(@b, ($keep_backups > $#b+1) ? $#b+1 : $keep_backups);
        }
    }

    warn ($opt{dryrun} ? "Pruning:\n" : "Pruned:\n") if $opt{verbose};
    foreach my $backup_name (@backups_to_delete) {
        warn "  $backup_name\n" if $opt{verbose};
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
    my $fobj = Brackup::DecryptedFile->new(filename => $tempfile, no_gpg => $opt->{no_gpg});

    return ( ($fobj->name || $tempfile), $fobj );
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
    # verbose
    # no_gpg

    my @backups = $self->backups;
    foreach my $i (0 .. $#backups) {
        my $backup = $backups[$i];
        warn sprintf "Reading backup %s [%d/%d]\n", $backup->filename, $i+1, scalar(@backups)
            if $opt->{verbose};
        my $parser;
        # Try local file
        if( $opt->{meta_dir} ){
            my $localfile = File::Spec->catfile($opt->{meta_dir}, $backup->filename);
            $parser = Brackup::Metafile->open($localfile) if(-e $localfile);
        }
        # If failed or not available:
        unless($parser){
            warn "Could not find local metafile; falling back to loading it from the target.\n" if $opt->{meta_dir};
            my ($filename, $fobj) = $self->get_and_decrypt_backup($backup->filename, $opt);
            $parser = Brackup::Metafile->open($filename);
        }
        my $is_header = 1;
        while (my $it = $parser->readline) {
            &$callback($it, $is_header, $backup->filename);
            $is_header = 0;
        }
    }

    return scalar(@backups);
}

sub fsck {
    my $self = shift;
    my $opts = shift;
    ##
    # meta_dir
    # verbose
    # dryrun

    my $label_dryrun = $opts->{dryrun} ? '(DRY RUN)' : '';
    warn "* Fsck starts... $label_dryrun\n";

    my $step;
    my %errors;
    my $gpg_rec;

    $step = 'i_meta_target';
    warn "* I. Collecting chunks in meta-files and checking them on the target $label_dryrun\n";

    # Get a list of chunks on the target with their size
    my $CHUNKS = $self->chunks_with_length;

    # Loop through all backup (meta) files and recreate the inventory into this hash
    my %INV;

    # Store names of used chunks here
    my %USEDCHUNKS;

    my $num_metafiles = $self->loop_items_in_backups (sub {
        my $item = shift;
        my $is_header = shift;
        my $backupname = shift;

        # Parse the metafile

        if($is_header){
            $gpg_rec = $item->{'GPG-Recipient'};
            return;
        }

        return unless $item->{Chunks}; # skip non-file entries

        my $filedigest = $item->{Digest} || die "Cannot find file digest in an item in metafile '$backupname'";
        my $singlechunk;
        my $no_filechunks = 0;

        foreach my $filechunk (split(/\s+/, $item->{Chunks})){
            $no_filechunks++;

            # {METASYNTAX} (search for this label to see where else this syntax is used) -- see Brackup::StoredChunk::to_meta

            my @labels = ('p_offset', 'p_length', 'range_or_s_length', 's_digest', 'p_digest');
            my %chunkdata = ();
            my $i = 0;
            foreach my $d (split(/;/, $filechunk)){
                $d =~ s/^\s+|\s+$//g;
                die "Cannot find '$labels[$i]' in '$filechunk' in metafile '$backupname'" unless defined $d;
                $chunkdata{$labels[$i]} = $d;
                $i++;
            }

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

            unless(exists $CHUNKS->{ $chunkdata{s_digest} }){
                warn "** Chunk '$chunkdata{s_digest}' referred to in metafile '$backupname' is missing from the target!\n" if $opts->{verbose};
                $errors{$step}++;
                warn "-- This chunk will be disregarded\n" if $opts->{verbose};
                next;
            }

            my $size_on_target = $CHUNKS->{ $chunkdata{s_digest} };
            if($chunkdata{s_range}){
                unless($size_on_target >= $chunkdata{s_range_to}){
                    if( $opts->{verbose} ){
                        warn "** Chunk '$chunkdata{s_digest}' referred to in metafile '$backupname' is too small on target to contain range!\n";
                        warn "-- Line in metafile: '$filechunk'\n";
                        warn "-- Size in metafile: '$chunkdata{s_length}'\n-- Size on target: '$size_on_target'\n";
                    }
                    $errors{$step}++;
                    warn "-- This chunk will be disregarded\n" if $opts->{verbose};
                    next;
                }
            }else{
                unless($size_on_target == $chunkdata{s_length}){
                    if( $opts->{verbose} ){
                        warn "** Chunk '$chunkdata{s_digest}' referred to in metafile '$backupname' has the wrong size on the target!\n";
                        warn "-- Line in metafile: '$filechunk'\n";
                        warn "-- Size in metafile: '$chunkdata{s_length}'\n-- Size on target: '$size_on_target'\n";
                    }
                    $errors{$step}++;
                    warn "-- This chunk will be disregarded\n" if $opts->{verbose};
                    next;
                }
            }

            # This chunk is used
            $USEDCHUNKS{ $chunkdata{s_digest} } = 1;

            # Create inventory entry in %INV
            # {INVSYNTAX} (search for this label to see where else this syntax is used)

            my $db_key = $chunkdata{p_digest};
            $db_key .= $gpg_rec ? ';to=' . $gpg_rec : ';raw';

            my $db_value = $chunkdata{s_digest} . ' ' . $chunkdata{s_length};
            $db_value .= ' ' . $chunkdata{s_range} if $chunkdata{s_range};

            $INV{$db_key} = $db_value;
        }

        if($singlechunk){
            die "ASSERT: Expected a one-chunk file based on metafile '$backupname'" unless $no_filechunks == 1;
        }

    }, $opts);

    # Abort if we haven't managed to collect any data because we haven't found any metafiles.
    # This can be due to a misconfiguration; for example, the naming prefix is wrong.
    if( (!$num_metafiles) && (!$opts->{ignore_no_meta}) ){
        die "Aborting as no metafiles were found. Use --ignore-no-metafiles to ignore this and continue.\n";
    }

    # Check the inventory
    # (At this point, all conflicts inside the target are already reported.)

    $step = 'ii_inv';
    warn "* II. Comparing the local inventory to the data collected $label_dryrun\n";

    my $label_curval = 'Description in inventory:';
    my $label_bkpval = 'Description from target :';

    while (my ($key, $curval) = $self->inventory_db->each) {
        my $bkpval = $INV{$key};
        if($bkpval){

            # WARNING %INV is destroyed in this process
            delete $INV{$key};

            unless($curval eq $bkpval){
                warn "** Mismatch between inventory and target for chunk '$key'\n" if $opts->{verbose};
                warn "-- $label_curval '$curval'\n-- $label_bkpval '$bkpval'\n" if $opts->{verbose} && $opts->{verbose} >= 2;
                $errors{$step}++;
                unless($opts->{dryrun}){
                    warn "-- Deleting from inventory to force re-upload\n" if $opts->{verbose};
                    $self->inventory_db->delete($key);
                }
            }
        }
        else{
            warn "** Chunk '$key' in inventory is missing or unused on target\n" if $opts->{verbose};
            warn "-- $label_curval '$curval'\n" if $opts->{verbose} && $opts->{verbose} >= 2;
            $errors{$step}++;
            unless($opts->{dryrun}){
                warn "-- Deleting from inventory\n" if $opts->{verbose};
                $self->inventory_db->delete($key);
            }
        }
    }

    foreach my $key (keys %INV){
        my $bkpval = $INV{$key};
        warn "** Chunk '$key' in target metafiles is missing from inventory\n" if $opts->{verbose};
        warn "-- $label_bkpval '$bkpval'\n" if $opts->{verbose} && $opts->{verbose} >= 2;
        $errors{$step}++;
        unless($opts->{dryrun}){
            warn "-- Adding to inventory\n" if $opts->{verbose};
            $self->inventory_db->set($key, $bkpval);
        }
    }


    $step = 'iii_gc';
    warn "* III. Garbage collection $label_dryrun\n";
    while(1){

        if( $opts->{skip_gc} ){
            warn "-- Skipped (--skip-gc was used)\n";
            last;
        }

        # It is not safe to continue if errors have been encountered in step 1,
        # as we might be deleting chunks that are in fact used or could be useful.
        if( $errors{i_meta_target} && ! $opts->{force_gc} ){
            warn "** It is unsafe to garbage collect as issues have been encountered during step I. Use --force-gc to override this.\n";
            last;
        }


        # Get orphaned chunks
        # WARNING %$CHUNKS is destroyed in this process
        foreach my $k (keys %USEDCHUNKS){
            die "ASSERT: chunk marked as used should exist" unless exists $CHUNKS->{$k};
            delete $CHUNKS->{$k};
        }

        $errors{$step} = scalar(keys %$CHUNKS);

        # No orphaned chunks?
        last unless $errors{$step};

        # On very verbose, print chunks to be deleted
        if( $opts->{verbose} && $opts->{verbose} >= 2 ){
            warn "* Oprhaned chunks:\n";
            foreach my $k (keys %$CHUNKS){
                warn "    $k Size: $CHUNKS->{$k}\n";
            }
        }

        # Dry run
        last if $opts->{dryrun};

        # Confirmation
        my $confirm = 'y';
        if($opts->{interactive}){
            printf "Run gc, removing %d orphaned chunks? [y/N] ", scalar $errors{$step};
            $confirm = <>;
        }
        last unless (lc substr($confirm,0,1) eq 'y');

        # Remove orhpaned chunks
        warn "Removing orphaned chunks\n" if $opts->{verbose};

        # (1) delete orphaned chunks from inventory and THEN from the target
        my $inventory_db = $self->inventory_db;
        while (my ($k, $v) = $inventory_db->each) {
            $v =~ s/ .*$//;         # strip value back to hash
            if(exists $CHUNKS->{$v}){
                $inventory_db->delete($k);
                $self->delete_chunk($v);
                delete $CHUNKS->{$v};
            }
        }

        # (2) delete chunks not found in the inventory
        foreach my $v (keys %$CHUNKS){
            $self->delete_chunk($v);
        }

        last;
    } # end while(1)

    # Print summary
    my %explanation = (
        '_meta_dir'     => "  * Wherever possible, locally stored metafiles have been used.\n",
        '_no_issues'    => "  * Success: No issues have been encountered.\n",
        '_all_fixed'    => "    These have all been fixed.\n",
        '_no_action'    => "    No action has been taken.\n",
        'i_meta_target' => "  * %d issues found while comparing metafiles to what is on the target.\n    These issues cannot be fixed; please consider deleting the affected backups.\n",
        'ii_inv'        => "  * %d issues found while comparing the inventory to metafiles and chunks.\n%s",
        'iii_gc'        => "  * %d issues found while collecting orphaned chunks.\n%s"
    );

    warn "* SUMMARY $label_dryrun\n";
    warn $explanation{_meta_dir} if $opts->{meta_dir};

    my $failed = undef;

    foreach my $k (sort keys %errors){
        next unless $errors{$k}; # skip 0 error counts
        $failed = 1;
        warn sprintf( $explanation{$k}, $errors{$k}, $opts->{dryrun} ? $explanation{_no_action} : $explanation{_all_fixed} );
    }

    warn $explanation{_no_issues} unless $failed;

    # Calculate a suitable return code (1=OK; see brackup-target for process return code)
    return 0 if $errors{i_meta_target}; # Failed: These errors cannot be fixed
    return 1 unless $failed; # Success: No issues found at all
    return 1 unless $opts->{dryrun}; # Success: we should have fixed all errors or died
    return 0; # Failed
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

=item B<gpg_daemons>

Specifies the maximum number of child processes used to encrypt chunks in parallel.

=back

=head1 SEE ALSO

L<Brackup>

L<Brackup::InventoryDatabase>

=cut

# vim:sw=4:et

