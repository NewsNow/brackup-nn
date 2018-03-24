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

package Brackup::Target::Amazon;
use strict;
use warnings;
use base 'Brackup::Target';
use Net::Amazon::S3 0.59;
use DateTime::Format::ISO8601;
use POSIX qw(_exit);
use Brackup::Target::Amazon::S3;

# fields in object:
#   s3  -- Net::Amazon::S3
#   access_key_id
#   sec_access_key_id
#   prefix
#   location
#   chunk_bucket : $self->{prefix} . "-chunks";
#   backup_bucket : $self->{prefix} . "-backups";
#   backup_prefix : added to the front of backup names when stored
#

sub new {
    my ($class, $confsec, $opts) = @_;
    my $self = $class->SUPER::new($confsec, $opts);

    $self->{access_key_id}     = $confsec->value("aws_access_key_id")
        or die "No 'aws_access_key_id'";
    $self->{sec_access_key_id} = $confsec->value("aws_secret_access_key")
        or die "No 'aws_secret_access_key'";
    $self->{prefix} = $confsec->value("aws_prefix") || $self->{access_key_id};
    $self->{location} = $confsec->value("aws_location") || undef;
    $self->{backup_prefix} = $confsec->value("backup_prefix") || undef;
    $self->{backup_path_prefix} = $confsec->value("backup_path_prefix") || ''; # suggested value 'backups/' is no longer here a default, for backwards compatibility.
    $self->{chunk_path_prefix} = $confsec->value("chunk_path_prefix") || ''; # suggested value 'chunks/' is no longer here a default, for backwards compatibility.
    $self->{multipart_threshold} = $confsec->byte_value("multipart_threshold") || 10*1024*1024;
    $self->{multipart_part_size} = $confsec->byte_value("multipart_part_size") || 10*1024*1024;
    die "multipart_part_size ($self->{multipart_part_size}) smaller than S3 minimum (5MB)\n" if $self->{multipart_part_size} < 5*1024*1024;
    
    $self->_common_s3_init;

    my $s3      = $self->{s3};

    my $buckets = $s3->buckets or die "Failed to get bucket list";

    unless (grep { $_->{bucket} eq $self->{chunk_bucket} } @{ $buckets->{buckets} }) {
        $s3->add_bucket({ bucket => $self->{chunk_bucket}, location_constraint => $self->{location} })
            or die "Chunk bucket creation failed\n";
    }

    unless (grep { $_->{bucket} eq $self->{backup_bucket} } @{ $buckets->{buckets} }) {
        $s3->add_bucket({ bucket => $self->{backup_bucket}, location_constraint => $self->{location} })
            or die "Backup bucket creation failed\n";
    }

    return $self;
}

sub _common_s3_init {
    my $self = shift;
    $self->{chunk_bucket}  = $self->{prefix} . "-chunks";
    $self->{backup_bucket} = $self->{prefix} . "-backups";
    $self->{s3}            = Net::Amazon::S3->new({
        aws_access_key_id     => $self->{access_key_id},
        aws_secret_access_key => $self->{sec_access_key_id},
        retry                 => 1,
        secure                => 1,
        host                  => $self->{location} ? ($self->{location} eq 'EU' ? 's3-eu-west-1.amazonaws.com' : "s3-$self->{location}.amazonaws.com") : 's3.amazonaws.com'
    });    
    $self->{s3c} = Brackup::Target::Amazon::S3->new(
         's3c' => Net::Amazon::S3::Client->new( s3 => $self->{s3} ),
         'multipart_threshold' => $self->{multipart_threshold},
         'multipart_part_size' => $self->{multipart_part_size},
         'verbose' => $self->{verbose}
     );
}

# ghetto
sub _prompt {
    my ($q) = @_;
    print "$q";
    my $ans = <STDIN>;
    $ans =~ s/^\s+//;
    $ans =~ s/\s+$//;
    return $ans;
}

# Location and backup_prefix aren't required for restores, so they're omitted here
sub backup_header {
    my ($self) = @_;
    return {
        "AWSAccessKeyID"    => $self->{access_key_id},
        "AWSPrefix"         => $self->{prefix},
        "ChunkPathPrefix"   => $self->{chunk_path_prefix}
    };
}

# Location and backup_prefix aren't required for restores, so they're omitted here
sub new_from_backup_header {
    my ($class, $header, $confsec) = @_;

    my $accesskey     = ($ENV{'AWS_KEY'} ||
                         $ENV{'AWS_ACCESS_KEY_ID'} ||
                         $header->{AWSAccessKeyID} ||
                         $confsec->value('aws_access_key_id') ||
                         _prompt("Your Amazon AWS access key? "))
        or die "Need your Amazon access key.\n";
    my $sec_accesskey = ($ENV{'AWS_SEC_KEY'} ||
                         $ENV{'AWS_ACCESS_KEY_SECRET'} ||
                         $confsec->value('aws_secret_access_key') ||
                         _prompt("Your Amazon AWS secret access key? "))
        or die "Need your Amazon secret access key.\n";
    my $prefix        = ($ENV{'AWS_PREFIX'} ||
                         $header->{AWSPrefix} || "");

    my $self = bless {}, $class;
    $self->{access_key_id}     = $accesskey;
    $self->{sec_access_key_id} = $sec_accesskey;
    $self->{prefix}            = $prefix || $self->{access_key_id};
    $self->{chunk_path_prefix} = $header->{ChunkPathPrefix} || "";
    $self->_common_s3_init;
    return $self;
}

sub has_chunk {
    my ($self, $chunk) = @_;
    my $dig = $chunk->backup_digest;   # "sha1:sdfsdf" format scalar

    my $res = eval { $self->{s3}->head_key({ bucket => $self->{chunk_bucket}, key => $self->chunkpath($dig) }); };
    return 0 unless $res;
    return 0 if $@ && $@ =~ /key not found/;
    return 0 unless $res->{content_type} eq "x-danga/brackup-chunk";
    return 1;
}

sub load_chunk {
    my ($self, $dig) = @_;
    my $bucket = $self->{s3}->bucket($self->{chunk_bucket});

    my $val = $bucket->get_key($self->chunkpath($dig))
        or return 0;
    return \ $val->{value};
}

sub store_chunk {
    my ($self, $schunk) = @_;

    return $self->daemonised_store_chunk($schunk);
}

sub _store_chunk {
    my ($self, $chunk) = @_;
    my $dig = $chunk->backup_digest;
    my $fh = $chunk->chunkref;
   
    # Uncomment to enable passing entire chunk by scalar ref,
    # as this seems to result in modest performance improvement
    # for chunksizes <= 10Mb.
    # N.B. This requires patched version of Net::Amazon::S3
    # that at time of writing doesn't support passing in scalar
    # references.
    # my $chunkref = do { local $/; <$fh> }; $fh = \$chunkref;

    return $self->{s3c}->put(
         bucket => $self->{chunk_bucket},
         key => $self->chunkpath($dig),
         value => $fh,
         headers => { content_type  => 'x-danga/brackup-chunk', 'x-amz-server-side-encryption' => 'AES256' }
     );
}

sub delete_chunk {
    my ($self, $dig) = @_;
    my $bucket = $self->{s3}->bucket($self->{chunk_bucket});
    return $bucket->delete_key($self->chunkpath($dig));
}

sub delete_chunk_multi {
    my ($self, $dig) = @_;
     push(@{$self->{_delete_chunk_multi}}, $self->chunkpath($dig));

     $self->delete_chunks_multi() if @{$self->{_delete_chunk_multi}} == 1000; # Amazon S3 multi-object delete limit.
}

sub delete_chunks_multi {
    my $self = shift;
    
    # If called at end of program to execute outstanding deletes, skip
    # if none were actually scheduled.
    return 1 unless exists $self->{_delete_chunk_multi} && scalar(@{$self->{_delete_chunk_multi}});
    
    printf "delete_chunk issuing delete_multi for %d chunks...", scalar(@{$self->{_delete_chunk_multi}});
    my $failed_keys = $self->{s3c}->delete_multi( bucket => $self->{chunk_bucket}, keys => $self->{_delete_chunk_multi} );

    if($failed_keys) {
        printf " but %d deletes failed.\n", scalar(@$failed_keys);
        $self->{_delete_chunk_multi} = $failed_keys;
        return 0;
    }

    print " ok.\n";
    delete $self->{_delete_chunk_multi};
    return 1;
}

# returns a list of names of all chunks
sub chunks {
    my $self = shift;

    my $chunks = $self->{s3}->list_bucket_all({ bucket => $self->{chunk_bucket} });
    my $prefix = $self->{chunk_path_prefix};

    return grep { $_ } map { $_->{key} =~ m!^\Q$prefix\E(.*)$! ? $1 : ''; } @{ $chunks->{keys} };
}

# Return a hashref { <NAME> => <LENGTH>, ... }
sub chunks_with_length {
    my $self = shift;

    my %r;

    my $chunks = $self->{s3}->list_bucket_all({ bucket => $self->{chunk_bucket} });
    my $prefix = $self->{chunk_path_prefix};

    foreach my $k (@{ $chunks->{keys} }){
        if($k->{key} =~ /^\Q$prefix\E(.*)$/){
            $r{$1} = $k->{size};
        }
    }

    return \%r;
}

sub store_backup_meta {
    my ($self, $name, $fh, $meta) = @_;

    $name = $self->{backup_prefix} . "-" . $name if defined $self->{backup_prefix};

    return 1 if
      eval {
            $fh = IO::File->new($meta->{filename},'r') unless $fh;
            return $self->{s3c}->put( bucket => $self->{backup_bucket}, key => $self->backuppath($name), 
                value => $fh,
                headers => { content_type => 'x-danga/brackup-meta', 'x-amz-server-side-encryption' => 'AES256' }
            );
        };

    die "Failed to store backup meta file $meta->{filename} to " . $self->backuppath($name) . " in $self->{backup_bucket}" . ($@ && " with exception $@") . "\n";
}

sub backups {
    my $self = shift;

    my @ret;
    my $backups = $self->{s3}->list_bucket_all({ bucket => $self->{backup_bucket} });
    my $prefix = $self->{backup_path_prefix};
    foreach my $backup (@{ $backups->{keys} }) {
        my $key = $backup->{key} =~ m!^\Q$prefix\E(.*)$! ? $1 : '';
        next unless $key;

        my $iso8601 = DateTime::Format::ISO8601->parse_datetime( $backup->{last_modified} );
        push @ret, Brackup::TargetBackupStatInfo->new($self, $key,
                                                      time => $iso8601->epoch,
                                                      size => $backup->{size});
    }
    return @ret;
}

sub get_backup {
    my $self = shift;
    my ($name, $output_file) = @_;

    my $bucket = $self->{s3}->bucket($self->{backup_bucket});
    my $val = $bucket->get_key($self->backuppath($name))
        or return 0;

    $output_file ||= $name;
    open(my $out, ">$output_file") or die "Failed to open $output_file: $!\n";
    my $outv = syswrite($out, $val->{value});
    die "download/write error" unless $outv == do { use bytes; length $val->{value} };
    close $out;
    return 1;
}

sub delete_backup {
    my $self = shift;
    my $name = shift;

    my $bucket = $self->{s3}->bucket($self->{backup_bucket});
    return $bucket->delete_key($self->backuppath($name));
}

sub chunkpath {
    my $self = shift;
    my $dig = shift;

    return $self->{chunk_path_prefix} . $dig;
}

sub backuppath {
    my $self = shift;
    my $name = shift;

    return $self->{backup_path_prefix} . $name;
}

sub size {
    my $self = shift;
    my $dig = shift;

    my $res = eval { $self->{s3}->head_key({ bucket => $self->{chunk_bucket}, key => $self->chunkpath($dig) }); };
    return 0 unless $res;
    return 0 if $@ && $@ =~ /key not found/;
    return 0 unless $res->{content_type} eq "x-danga/brackup-chunk";
    return $res->{content_length};
}

sub cleanup {
     my $self = shift;
     $self->{s3c}->abort_multipart_uploads( bucket => $self->{chunk_bucket} );
     $self->{s3c}->abort_multipart_uploads( bucket => $self->{backup_bucket} );
}

1;

=head1 NAME

Brackup::Target::Amazon - backup to Amazon's S3 service

=head1 EXAMPLE

In your ~/.brackup.conf file:

  [TARGET:amazon]
  type = Amazon
  aws_access_key_id  = ...
  aws_secret_access_key =  ....
  aws_prefix =  ....
  backup_prefix =  ....

=head1 CONFIG OPTIONS

All options may be omitted unless specified.

=over

=item B<type>

I<(Mandatory.)> Must be "B<Amazon>".

=item B<aws_access_key_id>

I<(Mandatory.)> Your Amazon Web Services access key id.

=item B<aws_secret_access_key>

I<(Mandatory.)> Your Amazon Web Services secret password for the above access key.  (not your Amazon password)

=item B<aws_prefix>

If you want to setup multiple backup targets on a single Amazon account you can
use different prefixes. This string is used to name the S3 buckets created by
Brackup. If not specified it defaults to the AWS access key id.

=item B<aws_location>

Sets the location constraint of the new buckets. If left unspecified, the
default S3 datacenter location will be used. Otherwise, you can set it
to 'EU' for an AWS European data center - note that costs are different.
This has only effect when your backup environment is initialized in S3 (i.e.
when buckets are created). If you want to move an existing backup environment
to another datacenter location, you have to delete its buckets before or create
a new one by specifing a different I<aws_prefix>.

=item B<backup_prefix>

When storing the backup metadata file to S3, the string specified here will
be prefixed onto the backup name. This is useful if you are collecting
backups from several hosts into a single Amazon S3 account but need to
be able to differentiate them; set your prefix to be the hostname
of each system, for example.

=back

=head1 SEE ALSO

L<Brackup::Target>

L<Net::Amazon::S3> -- required module to use Brackup::Target::Amazon

