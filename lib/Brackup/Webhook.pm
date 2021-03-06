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

package Brackup::Webhook;
use strict;
use warnings;
use Carp qw(croak);
use JSON;
use LWP::UserAgent;
use Sys::Hostname;
use POSIX qw(strftime);

sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;

    $self->{url}    = delete $opts{url}
      or croak "Missing required 'url' argument";
    $self->{root}   = delete $opts{root}
      or croak "Missing required 'root' argument";
    $self->{target} = delete $opts{target}
      or croak "Missing required 'target' argument";
    $self->{stats}  = delete $opts{stats}
      or croak "Missing required 'stats' argument";

    $self->{ua} = delete $opts{user_agent} || LWP::UserAgent->new(env_proxy => 1);

    croak("Unknown options: " . join(', ', keys %opts)) if %opts;

    $self->{data} = {
        hostname    => hostname,
        date        => strftime('%Y%m%d', localtime),
        root        => $self->{root}->name,
        target      => $self->{target}->name,
        stats       => $self->{stats}->as_hash,
    };

    return $self;
}

sub fire {
    my $self = shift;

    my $resp = $self->{ua}->post($self->{url},
        'Content-Type'  => 'application/json',
        Content         => encode_json($self->{data}),
    );

    # Just warn on failure?
    $resp->is_success
      or warn "webhook failure: " . $resp->code . ' ' . $resp->status_line;

    return $resp->code >= 200 && $resp->code <= 299;
}

sub dump {
    require Data::Dump;
    Data::Dump::dump $_[0]->{data};
}

1;

# vim:sw=4

