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
# This file is a new addition to brackup-nn, based on code in Net::Amazon::S3
# authored by Pedro Figueiredo <me@pedrofigueiredo.org>, copyright (c) 2013
# Amazon Digital Services, Leon Brocard, Brad Fitzpatrick, Pedro Figueiredo and
# licensed for use, modification and/or distribution under the same terms as the
# Perl 5 programming language system itself.
#

package Brackup::Target::Amazon::S3;
use strict;
use warnings;
use Net::Amazon::S3 0.59;
use Digest::MD5 qw(md5 md5_hex);
use MIME::Base64;
use IO::Handle;

sub new {
    my $class = shift;
    my %args = ref($_[0]) ? %{$_[0]} : @_;
    my $self = bless { %args }, $class;
    
    # Ensure sensible defaults
    $self->{multipart_threshold} ||= 10*1024*1024;
    $self->{multipart_part_size} ||= 10*1024*1024;
    return $self;
}

sub client {
    my $self = shift;
    return $self->{s3c};
}

sub _etag {
    my ( $self, $http_response ) = @_;
    my $etag = $http_response->header('ETag');
    if ($etag) {
        $etag =~ s/^\"//;
        $etag =~ s/\"$//;
    }
    return $etag;
}

sub _set_headers {
    my $self = shift;
    my $args = shift;
    
    my $md5        = $self->_md5($args);
    my $md5_hex    = unpack( 'H*', $md5 );
    my $md5_base64 = encode_base64($md5);
    chomp $md5_base64;

    $$args{headers}->{'Content-MD5'} = $md5_base64;
    $$args{headers}->{'Content-Length'} = $self->_length($args);
    
    return $md5_hex;
}

sub _md5 {
    my $self = shift;
    my $args = shift;
    
    # Scalar
    return md5( $$args{value} ) unless ref($$args{value});
    
    # Scalar reference
    return md5( ${$$args{value}} ) if ref($$args{value}) eq 'SCALAR';
    
    # Assume filehandle
    my $md5 = Digest::MD5->new();
    $$args{value}->seek(0,0);
    my $fh = $$args{value};
    while(<$fh>) {
        $md5->add($_);
    }
    $$args{value}->seek(0,0);
    return $md5->digest;
}

sub _length {
    my $self = shift;
    my $args = shift;
    
    # Scalar
    return length( $$args{value} ) unless ref($$args{value});
    
    # Scalar reference
    return length( ${$$args{value}} ) if ref($$args{value}) eq 'SCALAR';
    
    # Assume filehandle
    $$args{value}->seek(0, 2);
    my $l = $$args{value}->tell;
    $$args{value}->seek(0, 0);
    return $l; 
}

sub _part {
    my $self = shift;
    my $args = shift;
    my $start = shift;
    my $length = shift;
    
    # Scalar
    unless( ref($$args{value}) ) {
        my $value = substr($$args{value}, $start, $length);
        return \$value;
    }
    
    # Scalar reference
    if( ref($$args{value}) eq 'SCALAR' ) {
        my $value = substr(${$$args{value}}, $start, $length);
        return \$value;
    }
    
    # Assume filehandle
    return IO::InnerFile->new($$args{value}, $start, $length);
}

sub __content_sub {
    my $fh = shift;
    
    my $blksize = (ref($fh) eq 'IO::InnerFile') ? 4096 : ([$fh->stat]->[11] || 4096);
    
    $fh->seek(0, 0);
    
    return sub {
        my $buffer;
        my $read = $fh->read( $buffer, $blksize );
        unless( $read ) {
            # Restore filehandle to start of file (or IO::InnerFile)
            # in case sub is called again for a redirected request,
            # without a fresh __content_sub
            $fh->seek(0, 0);
            return '';
        }
        return $buffer;
    };
}

sub _data_ref {
    my $self = shift;
    my $args = shift;
    
    # Scalar
    return \$$args{value} unless ref($$args{value});
    
    # Scalar reference
    return $$args{value} if ref($$args{value}) eq 'SCALAR';
    
   # Assume filehandle
   return __content_sub($$args{value}); 
}

sub put {
    my $self = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};

    my $l = $self->_length($args);
    return $self->put_singlepart($args) if $self->_length($args) <= $self->{multipart_threshold};
    return $self->put_multipart($self->{multipart_part_size}, $args);
}

sub put_singlepart {
    my $self = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};

    return $self->_put('Net::Amazon::S3::Request::PutObject', $args);
}

sub put_part {
    my $self = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};
    
    return $self->_put('Net::Amazon::S3::Request::PutPart', $args);
}

sub put_multipart {
    my $self = shift;
    my $chunksize = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};
    
    die "ASSERT: Chunksize $chunksize smaller than S3 minimum 5MB\n" if $chunksize < 5*1024*1024;
    
    my $upload_id = $self->initiate_multipart_upload($args);

    my $data_length = $self->_length($args);
    
    my $part = 1;
    my @etags;
    my @parts;
    
    for(my $i=0; $i < $data_length; $i += $chunksize) {
    
        my $length = ($data_length - $i) > $chunksize ? $chunksize : ($data_length - $i);
        my $value = $self->_part($args, $i, $length );
        
        print "      * $$ putting part $part ($length/$data_length) ...\n" if $self->{verbose};
        my $put_part_response;
        unless( $put_part_response = $self->put_part(
              bucket => $$args{bucket},
              key => $$args{key},
              value => $value,
              upload_id      => $upload_id,
              part_number    => $part
        ) ) {
              $self->abort_multipart_upload(bucket => $$args{bucket}, key => $$args{key}, upload_id => $upload_id);
              return 0;
          }
         
        push(@etags, $put_part_response->header('ETag'));
        push(@parts, $part);
        $part++;
    }
    
    my $r = $self->complete_multipart_upload(
        bucket => $$args{bucket},
        key => $$args{key},
        upload_id       => $upload_id,
        etags           => \@etags,
        part_numbers    => \@parts
    );

    return 1;
}

sub initiate_multipart_upload {
    my $self = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};

    my $http_request = Net::Amazon::S3::Request::InitiateMultipartUpload->new(
        s3     => $self->client->s3,
        bucket => $$args{bucket},
        key => $$args{key},
        headers => $$args{headers}
    )->http_request;
    
    my $xpc = $self->client->_send_request_xpc($http_request);
    my $upload_id = $xpc->findvalue('//s3:UploadId');
    die "Couldn't get upload id from initiate_multipart_upload response XML" unless $upload_id;
    
    return $upload_id;
}

sub complete_multipart_upload {
    my $self = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};
    
    my $http_request =
      Net::Amazon::S3::Request::CompleteMultipartUpload->new(
          s3 => $self->client->s3,
          bucket => $$args{bucket},
          key => $$args{key},
          upload_id => $$args{upload_id},
          etags => $$args{etags},
          part_numbers => $$args{part_numbers},
      )->http_request;
    
    # TESTME: the line below and the return string shouldn't have been needed!
    # my $http_response = $self->client->_send_request($http_request);

    my $xpc = $self->client->_send_request_xpc($http_request);
    my $etag = $xpc->findvalue('//s3:ETag');
    die "Couldn't get ETag from complete_multipart_upload response XML" unless $etag;
    
    return 1; # $http_response;
}

sub _put {
    my $self = shift;
    my $class = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};
    
    my $md5_hex = $self->_set_headers($args);

    my $n_fails = 0;
    my $retries = 12;
    
    while ($n_fails < $retries) {
         my $http_response =
            eval {
                my %extra_args;
                foreach (qw( upload_id part_number )) {
                    $extra_args{$_} = $$args{$_} if exists $$args{$_};
                }
                
                my $http_request =
                  $class->new(
                      s3 => $self->client->s3,
                      bucket => $$args{bucket},
                      key => $$args{key},
                      headers => $$args{headers},
                      value => $self->_data_ref($args), 
                      %extra_args
                  )->http_request;
                
                my $http_response = $self->client->_send_request($http_request);
                
                return $http_response if $http_response->code == 200 && $self->_etag($http_response) eq $md5_hex;
                
                return undef;
            };
         
          return $http_response if $http_response;
     }
     continue {
        # transient failure?
         
          if(++$n_fails < $retries) {
              my $tosleep = $n_fails > 5 ? ( $n_fails > 10 ? 300 : 30 ) : 5;
              warn "Error uploading chunk [$@] ... will do retry \#$n_fails in $tosleep seconds ...\n";
              sleep $tosleep;
          }
     }
    
     warn "Unrecoverable error uploading chunk\n";
    return undef;
}

sub abort_multipart_upload {
    my $self    = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};
    
     my $http_request = Net::Amazon::S3::HTTPRequest->new(
         s3      => $self->client->s3,
         method  => 'DELETE',
         path    =>
         $$args{bucket} . '/' .
         $$args{key} .
         '?uploadId=' .
         $$args{upload_id}
     )->http_request;
    
    return $self->client->_send_request($http_request);
}

sub abort_multipart_uploads {
    my $self    = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};

    my $http_request = Net::Amazon::S3::HTTPRequest->new(
         s3      => $self->client->s3,
         method  => 'GET',
         path    => $$args{bucket} . "/?uploads",
     )->http_request;

    my $xpc = $self->client->_send_request_xpc($http_request);
    my @uploads = $xpc->findnodes('//s3:Upload');
    
    foreach my $upload (@uploads) {
        my $upload_id = $upload->getChildrenByTagName('UploadId');
        my $key = $upload->getChildrenByTagName('Key');
        print "Found incomplete multipart upload $upload_id for key $key, aborting...";
         
        $self->abort_multipart_upload(bucket => $$args{bucket}, key => $key, upload_id => $upload_id);
        print " done.\n";
    }
}

sub delete_multi {
    my $self    = shift;
    my $args = (ref($_[0]) eq 'HASH') ? $_[0] : {@_};

    my $http_request = Net::Amazon::S3::Request::DeleteMultiObject->new(
         s3      => $self->client->s3,
           bucket => $$args{bucket},
           keys => $$args{keys}
     )->http_request;
    
    my $xpc = $self->client->_send_request_xpc($http_request);
    
    return $$args{keys} unless $xpc->findnodes('//s3:DeleteResult');
    
    my $Keys;
    my @errors = $xpc->findnodes('//s3:Key');
    
    foreach my $object (@errors) {
         my $key = $object->to_literal;
         print "delete_multi failed to delete $$args{bucket}/$key\n";
         push(@$Keys, $key);
    }
    
    return $Keys;
}

1;
