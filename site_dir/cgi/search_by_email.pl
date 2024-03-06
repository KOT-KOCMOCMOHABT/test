#!/usr/bin/perl

# Ruso

use strict;
use warnings;
use utf8;
use open qw( :std :encoding(UTF-8) );

use DBI;
use Sys::Syslog;
#use Time::HiRes qw( CLOCK_MONOTONIC );
use CGI::Simple;

my $rows_limit_in_table = 100;
my $tr_color1 = "style='background-color: #d3d3d3'";
my $tr_color2 = "style='background-color: #fafafa'";

my $query = new CGI::Simple;
my $dbh;
my $msg;

# Логи
openlog(get_script_name(), 'ndelay,pid,nofatal', 'user');

# Получение данных POST
my $address = $query->param('email');
$address =~ s/\s//g;

print $query->header(-type=>'text/html', -charset=>'UTF8');
print '<h1 style="font-size:21px;">'.$address.'</h1>' ."\n";

# Сколько строк?
if ($address =~ /^\S+@\S+$/)
{
	# Похоже на email
	my $rows_c = get_row_counts($address);
	if ($rows_c)
	{
		# Вывод таблицы
		print_table($address, $rows_c);
	}
	else
	{
		# Данных нет
		print "No data.\n";
	}
}
else
{
	syslog('info', "[-] address: $address");
	print "Doesn't look like email.\n";
}

release_resources();
exit 0;

sub get_row_counts
{
	my $address = shift;
	my $sql = "SELECT count(1) FROM  
	(SELECT created, str, int_id FROM message WHERE int_id IN (SELECT int_id FROM log WHERE address = ?)
	union
	SELECT created, str, int_id FROM log WHERE address = ?)x";
	my $sth =dbh()->prepare($sql);
	my $rv = $sth->execute($address, $address);
        db_err($sth->errstr) if ($rv == -1);
	my $c = $sth->fetchrow_array();
	$sth->finish;
	syslog('info', "[+] address: $address, rows: $c");
	return $c;
}

sub print_table
{
	my $address = shift;
	my $rows_c = shift;
	my $sql = "SELECT x.created, x.str FROM
	(SELECT created, str, int_id FROM message WHERE int_id IN (SELECT int_id FROM log WHERE address = ?)
	union
	SELECT created, str, int_id FROM log WHERE address = ?)x ORDER BY x.int_id, x.created LIMIT $rows_limit_in_table";
	my $sth =dbh()->prepare($sql);
	my $rv = $sth->execute($address, $address);
	db_err($sth->errstr) if ($rv == -1);
	print '<table>';
	my $x = 0;
	while (my ($created, $str) = $sth->fetchrow_array())
	{
		$x++;
		if ($x%2)
		{
			print "<tr $tr_color1><td>$created</td><td>$str</td></tr>";
		}
		else
		{
			print "<tr $tr_color2><td>$created</td><td>$str</td>";
		}
	}
	$sth->finish;
	print "</table>\n";
	print '<h1 style="font-size:21px;">WARNING: The number of lines found exceeds specified limit - '.$rows_limit_in_table.' lines.</h1>' ."\n" if $rows_c > $rows_limit_in_table;
	return;
}

sub dbh
{
	unless ($dbh)
	{
		$dbh = DBI->connect("DBI:Pg:dbname=test888_db;host=127.0.0.1;port=5432;application_name=". get_script_name(), "test888", "test888",
		{'RaiseError' => 0, 'PrintError' => 0, pg_enable_utf8 => 1, InactiveDestroy => 1});
		db_err($DBI::errstr) if $DBI::err;
	}
	return $dbh;
}

sub db_err
{
	# Фиксатор ошибок коннекта
	my $err = shift;
	$err =~ s/\n/\./g; # Точка вместо возврата каретки
	$err =~ s/\s+/ /g; # Лишние пробелы убрать
	syslog('info', 'DB: '. $err);
	exit 0;
}

sub get_script_name
{
	return 'search_by_email';
}

sub release_resources
{
	# Отключение
	$dbh->disconnect() if $dbh;
	return;
}
