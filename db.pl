#!/usr/bin/perl -w

use strict;
use MongoDB;
use IPC::Open3;
use Symbol 'gensym';
use DateTime;

my $client = MongoDB::MongoClient->new(host => 'localhost', port => 27017);
my $db = $client->get_database('inverter');
my $values_col = $db->get_collection('values');
my $inverters_col = $db->get_collection('inverters');

$values_col->ensure_index({ day => 1,
			    inverter => 1 },
			  { unique => 1 });
$inverters_col->ensure_index({ serial => 1 },
			     { unique => 1 });

my($in, $out, $err);
$err = gensym;
{
  my $cmd = './opensunny ' . join(' ', @ARGV);
  my $pid = open3($in, $out, $err, $cmd);
  waitpid($pid, 0);
}

my @lines = readline($err);
my $inverter;
foreach my $line (@lines) {
  chomp($line);
  next unless ($line =~ s{^.*\[Value\] } {});

  if ($line =~ s{^Inverter found } {}) {
    $inverter = {};
    my @parts = split / /, $line;
    foreach my $part (@parts) {
      if ($part =~ m{^(.*?)=(.*)$}) {
	$inverter->{$1} = $2;
      }
    }

    my $serial = $inverter->{serial};
    $inverters_col->update({ serial => $serial },
			   { '$set' => $inverter },
			   { upsert => 1, safe => 1 }
			  );
    next;
  }

  my $values = {};
  my @parts = split / /, $line;
  foreach my $part (@parts) {
    if ($part =~ m{^(.*?)=(.*)$}) {
      my ($key, $value) = ($1, $2);
      if ($value =~ s{^(.*?\d+)[Kk].*$} {$1}) {
	$value *= 1000;
      } elsif ($value =~ s{^(.*?\d+)M.*$} {$1}) {
	$value *= 1000000;
      } elsif ($value =~ s{^(.*?\d+)G.*$} {$1}) {
	$value *= 1000000000;
      } else {
	$value =~ s{^(.*?\d+)\D+$} {$1};
      }
      if ($key eq 'timestamp') {
	$key = '_t';
	$value = DateTime->from_epoch(epoch => $value);
      }
      $values->{$key} = $value;
    }
  }

  if (defined $values->{_t}) {
    my $ts = $values->{_t};
    my $day = $ts->clone->set_hour(0)->set_minute(0)->set_second(0)->set_nanosecond(0);
    if ($values_col->count({ day => $day, inverter => $inverter->{serial}, 'readings._t' => $ts }) > 0) {
      my $set_values = {};
      while (my ($key, $value) = each %{$values}) {
	next if ($key eq '_t');
	$set_values->{'readings.$.' . $key} = $value;
      }
      $values_col->update({ day => $day, inverter => $inverter->{serial}, 'readings._t' => $ts },
			  { '$set' => $set_values,
			  },
			  { safe => 1 }
			 );
      exit;
    } elsif ($values_col->count({ day => $day, inverter => $inverter->{serial} }) > 0) {
      $values_col->update({ day => $day, inverter => $inverter->{serial} },
			  { '$push' => { readings => $values },
			  },
			  { safe => 1 }
			 );
    } else {
      $values_col->insert({ day => $day,
			    inverter => $inverter->{serial},
			    readings => [ $values ],
			  },
			  { safe => 1 });
    }
  } elsif (defined $inverter->{serial}) {
    my $serial = $inverter->{serial};
    $inverters_col->update({ serial => $serial },
			   { '$set' => $values },
			   { upsert => 1, safe => 1 }
			  );
  }
}
