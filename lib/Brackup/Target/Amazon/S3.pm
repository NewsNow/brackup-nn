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

package Brackup::Target::Amazon::S3;
use strict;
use warnings;
use Net::Amazon::S3 0.59;
use Digest::MD5 qw(md5 md5_hex);
use MIME::Base64;

sub new {
    my $class = shift;
    my %args = ref($_[0]) ? %{$_[0]} : @_;
    my $self = bless { %args }, $class;
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
    
    my $md5        = md5($$args{value});
    my $md5_hex    = unpack( 'H*', $md5 );
    my $md5_base64 = encode_base64($md5);
    chomp $md5_base64;

    $$args{headers}->{'Content-MD5'} = $md5_base64;
    $$args{headers}->{'Content-Length'} = length $$args{value} if defined $$args{value};
    
    return $md5_hex;
}

sub put {
    my $self = shift;
    
    # return $self->put_singlepart(@_);
    return $self->put_multipart(1048576*5, @_);
}

sub put_singlepart {
    my $self = shift;
    my %args = ref($_[0]) ? %{$_[0]} : @_;

    my $md5_hex = $self->_set_headers(\%args);
    
    my $http_request = Net::Amazon::S3::Request::PutObject->new(
        s3        => $self->client->s3,
        %args
    )->http_request;
    
    my $http_response = $self->client->_send_request($http_request);
    
    return 0 if $http_response->code != 200 || $self->_etag($http_response) ne $md5_hex;
    
    return $http_response;  
}

sub put_multipart {
    my $self = shift;
    my $chunksize = shift;
    
    die "ASSERT: Chunksize $chunksize smaller than S3 minimum 5MB\n" if $chunksize < 5*1024*1024;
    
    my %args = ref($_[0]) ? %{$_[0]} : @_;
    
    my $upload_id = $self->initiate_multipart_upload(%args);

    my $data = $args{value};
    my $part = 1;
    my @etags;
    my @parts;
    while(length($data)) {
    
        my $value = substr $data, 0, $chunksize, '';
        
        print STDERR "Putting part $part ... \n";
        my $put_part_response = $self->put_part(
            bucket => $args{bucket},
            key => $args{key},
            value => $value,
            upload_id      => $upload_id,
            part_number    => $part
        );
        
        # FIXME: Add retry logic around here.
        # See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html for details
        
        push(@etags, $put_part_response->header('ETag'));
        push(@parts, $part);
        $part++;
    }
    
    my $r = $self->complete_multipart_upload(
        %args,
        upload_id       => $upload_id,
        etags           => \@etags,
        part_numbers    => \@parts
    );

    return 1;
}

sub initiate_multipart_upload {
    my $self = shift;
    my %args = ref($_[0]) ? %{$_[0]} : @_;
    
    my $http_request = Net::Amazon::S3::Request::InitiateMultipartUpload->new(
        s3     => $self->client->s3,
        bucket => $args{bucket},
        key => $args{key},
        headers => $args{headers}
    )->http_request;
    
    my $xpc = $self->client->_send_request_xpc($http_request);
    my $upload_id = $xpc->findvalue('//s3:UploadId');
    die "Couldn't get upload id from initiate_multipart_upload response XML" unless $upload_id;
    
    return $upload_id;
}

sub put_part {
    my $self = shift;
    my %args = ref($_[0]) ? %{$_[0]} : @_;
    
    my $md5_hex = $self->_set_headers(\%args);
    
    my $http_request =
      Net::Amazon::S3::Request::PutPart->new(
          s3 => $self->client->s3,        
          %args
      )->http_request;
    
    my $http_response = $self->client->_send_request($http_request);
    
    return 0 if $http_response->code != 200 || $self->_etag($http_response) ne $md5_hex;
    
    return $http_response;
}

sub complete_multipart_upload {
    my $self = shift;
    my %args = ref($_[0]) ? %{$_[0]} : @_;
    
    my $http_request =
      Net::Amazon::S3::Request::CompleteMultipartUpload->new(
          s3 => $self->client->s3,
          bucket => $args{bucket},
          key => $args{key},
          upload_id => $args{upload_id},
          upload_id => $args{upload_id},
          etags => $args{etags},
          part_numbers => $args{part_numbers},
      )->http_request;
    
    my $http_response = $self->client->_send_request($http_request);

    my $xpc = $self->client->_send_request_xpc($http_request);
    my $etag = $xpc->findvalue('//s3:ETag');
    die "Couldn't get ETag from complete_multipart_upload response XML" unless $etag;
    
    return $http_response;
}

1;
