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

package Brackup::Root;
use strict;
use warnings;
use Carp qw(croak);
use File::Find;
use File::Spec;
use Brackup::DigestCache;
use Brackup::Util qw(io_print_to_fh);
use IPC::Open2;
use Symbol;

sub new {
    my ($class, $conf) = @_;
    my $self = bless {}, $class;

    ($self->{name}) = $conf->name =~ m/^SOURCE:(.+)$/
        or die "No backup-root name provided.";
    die "Backup-root name must be only a-z, A-Z, 0-9, and _." unless $self->{name} =~ /^\w+/;

    $self->{dir}        = $conf->path_value('path');
    $self->{gpg_path}   = $conf->value('gpg_path') || "gpg";
    $self->{gpg_rcpt}   = [ $conf->values('gpg_recipient') ];
    $self->{chunk_size} = $conf->byte_value('chunk_size');
    $self->{ignore}     = [];
    $self->{accept}     = [];

    $self->{smart_mp3_chunking} = $conf->bool_value('smart_mp3_chunking');

    $self->{merge_files_under}  = $conf->byte_value('merge_files_under');
    $self->{max_composite_size} = $conf->byte_value('max_composite_chunk_size') || 2**20;

    die "'max_composite_chunk_size' must be greater than 'merge_files_under'\n" unless
        $self->{max_composite_size} > $self->{merge_files_under};

    $self->{gpg_args}   = [];  # TODO: let user set this.  for now, not possible

    $self->{digcache}   = Brackup::DigestCache->new($self, $conf);
    $self->{digcache_file} = $self->{digcache}->backing_file;  # may be empty, if digest cache doesn't use a file

    $self->{noatime}     = $conf->value('noatime');
    $self->{webhook_url} = $conf->value('webhook_url');

    return $self;
}

sub merge_files_under  { $_[0]{merge_files_under}  }
sub max_composite_size { $_[0]{max_composite_size} }
sub smart_mp3_chunking { $_[0]{smart_mp3_chunking} }
sub webhook_url        { $_[0]{webhook_url} }

sub gpg_path {
    my $self = shift;
    return $self->{gpg_path};
}

sub gpg_args {
    my $self = shift;
    return @{ $self->{gpg_args} };
}

sub gpg_rcpts {
    my $self = shift;
    return @{ $self->{gpg_rcpt} };
}

# returns Brackup::DigestCache object
sub digest_cache {
    my $self = shift;
    return $self->{digcache};
}

sub chunk_size {
    my $self = shift;
    return $self->{chunk_size} || (64 * 2**20);  # default to 64MB
}

sub publicname {
    # FIXME: let users define the public (obscured) name of their roots.  s/porn/media/, etc.
    # because their metafile key names (which contain the root) aren't encrypted.
    return $_[0]{name};
}

sub name {
    return $_[0]{name};
}

sub ignore {
    my ($self, $pattern) = @_;
    push @{ $self->{ignore} }, qr/$pattern/;
}

sub accept {
    my ($self, $pattern) = @_;

    $pattern =~ s/(^\s+)|(\s+$)//g;

    if($pattern =~ s/^([!=])(?:=?)\s*//) {
        my $cond = $1;

        # Do not allow double slashes
        die "'accept' contains '//'" if $pattern =~ m{//};

        $pattern =~ s{^/}{}; # Remove / from the beginning of rules

        # Create a list of regexps to test for all parents of the current path
        my @parentregexps;
        my $fileregexp;

        # split() does not honor trailing separators, so check and remove trailing slashes
        my $trailingslash;
        $trailingslash = ($pattern =~ m{/$});
        $pattern =~ s{/$}{};

        foreach my $patternpart (split(m{/}, $pattern)){
            $fileregexp .= '/' if $fileregexp;
            $fileregexp .= quotemeta($patternpart);

            # Replace '*' in the pattern
            my $plus = '[^/]+';
            $fileregexp =~ s{^\\\*$}{$plus};
            $fileregexp =~ s{^\\\*/}{$plus/};
            $fileregexp =~ s{/\\\*$}{/$plus};
            $fileregexp =~ s{/\\\*/}{/$plus/}g;
            $fileregexp =~ s{\\\*}{[^/]*}g;

            push @parentregexps, '^' . $fileregexp . '/$';
        }

        # We don't need the last parent regexp
        pop(@parentregexps);

        # Ensure that the main regexp ends in either / or $
        $fileregexp = '^' . $fileregexp . ($trailingslash ? '/' : '$');

        push @{ $self->{accept} }, [ $cond eq '=' ? 1 : 0, $fileregexp, \@parentregexps ];
    }
}

sub path {
    return $_[0]{dir};
}

sub noatime {
    return $_[0]{noatime};
}

sub foreach_file {
    my ($self, $cb) = @_;

    chdir $self->{dir} or die "Failed to chdir to $self->{dir}";

    my %statcache; # file -> statobj

    find({
        no_chdir => 1,
        preprocess => sub {
            my $dir = $File::Find::dir;
            my $digcache_file = $self->{digcache_file};
            my @good_dentries;
          DENTRY:
            foreach my $dentry (@_) {
                next if $dentry eq "." || $dentry eq "..";

                # This is relative to $self->{dir} as we've chdir'd there
                my $path = "$dir/$dentry";

                # skip the digest database file.  not sure if this is smart or not.
                # for now it'd be kinda nice to have, but it's re-creatable from
                # the backup meta files later, so let's skip it.
                if($digcache_file) {
                    my $fullpath = File::Spec->catfile( $self->{dir}, $dir, $dentry ); # We presuppose that this is a file
                    next if $fullpath =~ m!\Q$digcache_file\E(-journal)?$!s;
                }

                $path =~ s!^\./!!;

                # GC: seems to work fine as of at least gpg 1.4.5, so commenting out
                # gpg seems to barf on files ending in whitespace, blowing
                # stuff up, so we just skip them instead...
                #if ($self->gpg_rcpts && $path =~ /\s+$/) {
                #    warn "Skipping file ending in whitespace: <$path>\n";
                #    next;
                #}

                my $statobj = File::stat::lstat($path);
                my $is_dir = -d _;

                foreach my $pattern (@{ $self->{ignore} }) {
                    next DENTRY if $path =~ /$pattern/;
                    next DENTRY if $is_dir && "$path/" =~ /$pattern/;
                }

                if(@{ $self->{accept} }) {
                    my $npath = $path;
                    $npath .= '/' if $is_dir;

                    my $dbg = "$npath:\n";

                    my $rule_outcome = 0;
                    foreach my $rule (@{ $self->{accept} }) {
                        my $cond = $rule->[0];
                        my $filepattern = $rule->[1];
                        my $parentpatterns = $rule->[2];
                        $dbg .= "  (file) " . ($cond ? '=~' : '!~') . " $filepattern\n";
                        if($npath =~ /$filepattern/) {
                            $dbg .= "    (MATCH:F)\n";
                            $rule_outcome = $cond;
                        }
                        elsif($cond && $is_dir) { # only if accept line is == (accepting) and npath is dir
                            foreach my $parentpattern (@$parentpatterns){
                                $dbg .= "  (parent) " . $parentpattern . "\n";
                                if($npath =~ /$parentpattern/) {
                                    $dbg .= "    (MATCH:P)\n";
                                    $rule_outcome = $cond;
                                }
                            }
                        }
                    }
                    # print STDERR ($rule_outcome ? 'Y ' : 'N ' ) . $dbg;

                    next DENTRY unless $rule_outcome;
                }

                $statcache{$path} = $statobj;
                push @good_dentries, $dentry;
            }

            # to let it recurse into the good directories we didn't
            # already throw away:
            return sort @good_dentries;
        },

        wanted => sub {
            my $path = $_;
            $path =~ s!^\./!!;

            my $stat_obj = delete $statcache{$path};
            my $file = Brackup::File->new(root => $self,
                                          path => $path,
                                          stat => $stat_obj,
                                          );
            $cb->($file);
        },
    }, ".");
}

sub as_string {
    my $self = shift;
    return $self->{name} . "($self->{dir})";
}

sub du_stats {
    my $self = shift;

    my $show_all = $ENV{BRACKUP_DU_ALL};
    my @dir_stack;
    my %dir_size;
    my $pop_dir = sub {
        my $dir = pop @dir_stack;
        printf("%-20d%s\n", $dir_size{$dir} || 0, $dir);
        delete $dir_size{$dir};
    };
    my $start_dir = sub {
        my $dir = shift;
        unless ($dir eq ".") {
            my @parts = (".", split(m!/!, $dir));
            while (@dir_stack >= @parts) {
                $pop_dir->();
            }
        }
        push @dir_stack, $dir;
    };
    $self->foreach_file(sub {
        my $file = shift;
        my $path = $file->path;
        if ($file->is_dir) {
            $start_dir->($path);
            return;
        }
        if ($file->is_file) {
            my $size = $file->size;
            my $kB   = int($size / 1024) + ($size % 1024 ? 1 : 0);
            printf("%-20d%s\n", $kB, $path) if $show_all;
            $dir_size{$_} += $kB foreach @dir_stack;
        }
    });

    $pop_dir->() while @dir_stack;
}

# given filehandle to data, returns encrypted data
sub encrypt {
    my ($self, $data_fh, $outfn) = @_;
    my @gpg_rcpts = $self->gpg_rcpts
        or Carp::confess("Encryption not setup for this root");

    my $cout = Symbol::gensym();
    my $cin = Symbol::gensym();

    my @recipients = map {("--recipient", $_)} @gpg_rcpts;
    my $pid = IPC::Open2::open2($cout, $cin,
        $self->gpg_path, $self->gpg_args,
        @recipients,
        "--trust-model=always",
        "--batch",
        "--encrypt",
        "--output", $outfn,
        "--yes",
        "-"                 # read from stdin
    );

    # send data to gpg
    binmode $cin;
    my $bytes = io_print_to_fh($data_fh, $cin)
      or die "Sending data to gpg failed: $!";

    close $cin;
    close $cout;

    waitpid($pid, 0);
    die "GPG failed: $!" if $? != 0; # If gpg return status is non-zero
}

1;

__END__

=head1 NAME

Brackup::Root - describes the source directory (and options) for a backup

=head1 EXAMPLE

In your ~/.brackup.conf file:

  [SOURCE:bradhome]
  path = /home/bradfitz/
  gpg_recipient = 5E1B3EC5
  chunk_size = 64MB
  ignore = ^\.thumbnails/
  ignore = ^\.kde/share/thumbnails/
  ignore = ^\.ee/(minis|icons|previews)/
  ignore = ^build/
  noatime = 1
  webhook_url = http://example.com/hook

=head1 CONFIG OPTIONS

=over

=item B<path>

The directory to backup (recursively)

=item B<accept>

Use one or more 'accept' lines to control what files to include in the backup set.

  path = /home/
  accept = == user/src/*/*.pm
  accept = != user/src/oldproject

Depending on whether 'accept = ' is followed by '==' or '!=', the accept line
includes or excludes the files or directories it matches.
Accept lines are checked in the order they appear, and the last line that
matches the current file or directory determines whether it is included in the backup set.
If one accept line is present in the configuration, then only those files and directories
are included which are accepted during this process; that is, one needs to have at least
one '==' line.

In the pattern that follows '==' or '!=', no character except '*' is special.
'*' stands for zero or more characters except a slash,
or one or more characters except a slash where zero characters would lead to a double slash
(at the beginning or end of the pattern or between two slashes).

A trailing slash on a pattern implies accepting everything under that directory.
For example, '== home/user/' includes 'home/user/file', but '== home/user' only
includes the file 'home/user'.

Including a file or directory in the backup set entails including all its parent directories.

=item B<gpg_recipient>

The public key signature to encrypt data with.  See L<Brackup::Manual::Overview/"Using encryption">.

=item B<chunk_size>

In units of bytes, kB, MB, etc.  The max size of a chunk to be stored
on the target.  Files over this size are cut up into chunks of this
size or smaller.  The default is 64 MB if not specified.

=item B<ignore>

Perl5 regular expression of files not to backup.  You may have multiple ignore lines.

=item B<noatime>

If true, don't backup access times.  They're kinda useless anyway, and
just make the *.brackup metafiles larger.

=item B<merge_files_under>

In units of bytes, kB, MB, etc.  If files are under this size.  By
default this feature is off (value 0), purely because it's new, but 1
kB is a recommended size, and will probably be the default in the
future.  Set it to 0 to explicitly disable.

=item B<max_composite_chunk_size>

In units of bytes, kB, MB, etc.  The maximum size of a composite
chunk, holding lots of little files.  If this is too big, you'll waste
more space with future iterative backups updating files locked into
this chunk with unchanged chunks.

Recommended, and default value, is 1 MB.

=item B<smart_mp3_chunking>

Boolean parameter.  Set to one of {on,yes,true,1} to make mp3 files
chunked along their metadata boundaries.  If a file has both ID3v1 and
ID3v2 chunks, the file will be cut into three parts: two little ones
for the ID3 tags, and one big one for the music bytes.

=item B<inherit>

The name of another Brackup::Root section to inherit from i.e. to use
for any parameters that are not already defined in the current section.
The example above could also be written:

  [SOURCE:defaults]
  chunk_size = 64MB
  noatime = 0

  [SOURCE:bradhome]
  inherit = defaults
  path = /home/bradfitz/
  gpg_recipient = 5E1B3EC5
  ignore = ^\.thumbnails/
  ignore = ^\.kde/share/thumbnails/
  ignore = ^\.ee/(minis|icons|previews)/
  ignore = ^build/
  noatime = 1

=item B<webhook_url>

URL to be POSTed to upon backup completion. The post payload is a json
object with 'root', 'target', and 'stats' members, with the first two
being the source and target name strings, and 'stats' being a serialised
L<Brackup::BackupStats> object.

=back
