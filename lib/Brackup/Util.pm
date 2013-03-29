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

package Brackup::Util;
use strict;
use warnings;
require Exporter;

use vars qw(@ISA @EXPORT_OK);
@ISA = ('Exporter');
@EXPORT_OK = qw(tempfile tempfile_obj tempdir slurp valid_params noclobber_filename io_print_to_fh io_sha1);

use File::Path qw();
use File::Spec;
use Carp;
use Fcntl qw(O_RDONLY);
use Digest::SHA1;
use Brackup::Config;

our $mainpid = $$;
our $SHUTDOWN_REQUESTED = 0;
my $_temp_directory;

END {
    # will happen after File::Temp's cleanup
    if ($$ == $mainpid and $_temp_directory) {
        File::Path::rmtree($_temp_directory, 0, 1) unless $ENV{BRACKUP_TEST_NOCLEANUP};
    }
}
use File::Temp ();

sub setup_sig_handlers {
    $SIG{'TERM'} = \&brackup_destruct;
    $SIG{'INT'}  = \&brackup_destruct;
}

sub brackup_destruct {
    if($mainpid == $$){
        warn "Shutdown requested...\n";
    }
    $SHUTDOWN_REQUESTED = 1;
}

sub _get_temp_directory {
    # Create temporary directory if we need one. By default, all temporary
    # files will be placed in it.
    unless (defined($_temp_directory)) {
        my %tmpdiropts = (
            CLEANUP => $ENV{BRACKUP_TEST_NOCLEANUP} ? 0 : 1
        );

        my $local_tmp = $Brackup::Config::CONFIG{'target'}->{'local_tmp'};
        if($local_tmp){
            $tmpdiropts{DIR} = $local_tmp;
        }

        $_temp_directory = File::Temp::tempdir(%tmpdiropts);
    }

    return $_temp_directory;
}

sub tempfile {
    my (@ret) = File::Temp::tempfile(DIR => _get_temp_directory(),
                                     EXLOCK => 0,
                                    );
    return wantarray ? @ret : $ret[0];
}

sub tempfile_obj {
    return File::Temp->new(DIR => _get_temp_directory(),
                           EXLOCK => 0,
                           UNLINK => $ENV{BRACKUP_TEST_NOCLEANUP} ? 0 : 1,
                          );
}

# Utils::tempdir() accepts the same options as File::Temp::tempdir.
sub tempdir {
    my %options = @_;
    $options{DIR} ||= _get_temp_directory();
    return File::Temp::tempdir(%options);
}

sub slurp {
    my $file = shift;
    my %opts = @_;
    my $fh;
    if ($opts{decompress} and eval { require IO::Uncompress::AnyUncompress }) {
        $fh = IO::Uncompress::AnyUncompress->new($file)
            or die "Failed to open file $file: $IO::Uncompress::AnyUncompress::AnyUncompressError";
    } else {
        sysopen($fh, $file, O_RDONLY) or die "Failed to open $file: $!";
    }
    return do { local $/; <$fh>; };
}

sub valid_params {
    my ($vlist, %uarg) = @_;
    my %ret;
    $ret{$_} = delete $uarg{$_} foreach @$vlist;
    croak("Bogus options: " . join(', ', sort keys %uarg)) if %uarg;
    return %ret;
}

# Uniquify the given filename to avoid clobbering existing files
sub noclobber_filename {
    my ($filename) = @_;
    return $filename if ! -e $filename;
    for (my $i = 1; ; $i++) {
        return "$filename.$i" if ! -e "$filename.$i";
    }
}

# Prints all data from an IO::Handle to a filehandle
sub io_print_to_fh {
    my ($io_handle, $fh, $sha1) = @_;
    my $buf;
    my $bytes = 0;

    while($io_handle->read($buf, 4096)) {
        print $fh $buf;
        $bytes += length $buf;
        $sha1->add($buf) if $sha1;
    }

    return $bytes;
}

# computes sha1 of data in an IO::Handle
sub io_sha1 {
    my ($io_handle) = @_;

    my $sha1 = Digest::SHA1->new;
    my $buf;

    while($io_handle->read($buf, 4096)) {
        $sha1->add($buf);
    }

    return $sha1->hexdigest;
}

sub unix2human {
   my ($t) = @_;
   my @GMT = gmtime($t);
   return sprintf( "%04d%02d%02d%02d%02d%02d", $GMT[5] + 1900, $GMT[4] + 1, @GMT[ 3, 2, 1, 0 ] );
}

sub human2unix {
    my $s = shift;

   my ($Tm) = @_;

   # Remove all non-numeric characters
   $Tm =~ s/\D//g;

   # Extract year, month, day, hours, mins and secs from date string
   my ( $year, $mon, $day, $hour, $min, $sec ) = $Tm =~ /^(\d\d\d\d)(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)$/;

   # If the regular expression didn't match, then we don't have a valid date string.
   return undef unless $&;

   # Convert date elements above into Unix timestamp. For 'explanation'
   # of this see this Linux patch description:
   #
   #   http://www.linuxhq.com/kernel/v2.2/patch/patch-2.2.18/linux_arch_m68k_mac_config.c.html

   if( 0 >= ( $mon -= 2 ) ) {    # /* 1..12 -> 11,12,1..10 */
      $mon += 12;                # /* Puts Feb last since it has leap day */
      $year -= 1;
   }
   return (
      ( ( ( int( $year / 4 ) - int( $year / 100 ) + int( $year / 400 ) + int( 367 * $mon / 12 ) + $day ) + $year * 365 - 719499 ) * 24 + $hour    # /* now have hours */
      ) * 60 + $min                                                                                                                                # /* now have minutes */
   ) * 60 + $sec;                                                                                                                                  # /* finally seconds */
}

sub fix_meta_dir {
    my $class = shift;
    my $meta_dir = shift;
    my $cwd = shift;
    my $default_to_cwd = shift;

    if($meta_dir){
        # If meta_dir is relative, derive new meta_dir path from current working directory.
        return ($meta_dir =~ /^\//) ? $meta_dir : File::Spec->catdir($cwd, $meta_dir);
    }

    return $cwd if $default_to_cwd;
    return undef;
}

1;

# vim:sw=4
