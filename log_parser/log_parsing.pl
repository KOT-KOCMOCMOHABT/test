#!/usr/bin/perl

# Для парсинга файла и заполнения базы
# Ruso

use strict;
use warnings;
use utf8;
use DBI;
use Sys::Syslog;
use Time::HiRes qw( CLOCK_MONOTONIC );

my $dbh; 
my $file_to_parsing = 'out';

my $length_int_id_db = 16;				# CHAR(16) NOT NULL
my $logs_timestamp_format = 'YYYY-MM-DD HH24:MI:SS';	# Формат записи времени в логах

# Для статистики
my $count_rows = 0; 		# Кол-во прочитанных строк
my $count_message_rows = 0;	# Кол-во строк занесенных в таблицу message
my $count_log_rows = 0;		# Кол-во строк занесенных в таблицу log
my $count_invalid_rows = 0;	# Кол-во не валидных строк - (см: sub chk_int_id)

# Логи
openlog(get_script_name(), 'ndelay,pid,nofatal', 'daemon');

# Перед заполнение таблиц - проверим, вдруг не пусты
exit 0 if tables_is_not_empty();

# Подготовка запросов в БД
my $sth_m =dbh()->prepare("INSERT INTO public.message (created, id, int_id, str) VALUES (to_timestamp(?, '$logs_timestamp_format'), ?, ?, ?)");
my $sth_l =dbh()->prepare("INSERT INTO public.log (created, int_id, str, address) VALUES(to_timestamp(?, '$logs_timestamp_format'), ?, ?, ?)");

# Парсинг
read_file($file_to_parsing);

# Освобождение дескрипторов бд
$sth_m->finish;
$sth_l->finish;

release_resources();
exit 0;

sub read_file
{
	# Прочитать и занести в базу
	my $file = shift;
	my $start_time = monotime();

	open my $fh, '<:encoding(UTF-8)', $file or die "Could not open file '$file' $!\n";
	syslog('info', "Start parsing file $file");
	print "Start parsing file $file\n";
	while (my $row = <$fh>)
	{
		$count_rows++;
		chomp $row;

		my $created = '';
		my $int_id = '';
		my $flag = '';
		my $id = '';
		my $str = '';
		my $address = '';
		my $log_tail = '';

		$row =~ /(\d{4}-\d\d-\d\d\s\d\d:\d\d:\d\d)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(.*)/;
		
		if (!$1)
		{
			# Лог короткий, флага нет, адреcа нет
			$row =~ /(\d{4}-\d\d-\d\d\s\d\d:\d\d:\d\d)\s(\S*)\s(.*)/;
			$created = $1;
			$int_id = $2;
			
			# Проверка int_id на валидность, ибо есть строки в который нет int_id
			next unless chk_int_id($int_id, $row);

			$str = "$2 $3";
			insert_into_log_table($created, $int_id, $str, $address);
		}
		else
		{
			# Полная строка лога
			$created = $1;
			$int_id = $2;
			
			# Проверка int_id на валидность, ибо есть строки в который нет int_id
			next unless chk_int_id($int_id, $row);

			$flag = $3;
			$address = $4;
			$log_tail = $6;

			$str = "$int_id $flag $address $5 $log_tail"; # Логи (без временной метки)

			if ($flag eq '<=')
			{
				# В таблицу message
				# created - timestamp строки лога
				# id - значение поля id=xxxx из строки лога
				# int_id - внутренний id сообщения
				# str - строка лога (без временной метки)
				
				if ($address eq '<>')
				{
					# В этом логе нет id= - в таблицу логи
					$address = ''; # Возможно это лишнее, но ведь <> - это не адрес?
					insert_into_log_table($created, $int_id, $str, $address);
				}
				else
				{
					$log_tail =~ /.*id=(\S*)$/;
					$id = $1;
					insert_into_message($created, $id, $int_id, $str);
				}
			}
			else
			{
				$address = '' if (($flag ne '=>') && ($flag ne '->') && ($flag ne '**') && ($flag ne '=='));
				if ($flag eq '**')
				{
					# Заменить fwxvparobkymnbyemevz@london.com: на fwxvparobkymnbyemevz@london.com
					$address =~ s/://g;
				}
				if ($address eq ':blackhole:')
				{
					# Заменить :blackhole: <tpxmuwr@somehost.ru>'
					# на tpxmuwr@somehost.ru
					$address = $5;
					$address =~ s/<|>//g;
				}
				insert_into_log_table($created, $int_id, $str, $address);
			}
		}
	}
	close $fh;

	my $stop_time = monotime();
	my $delta_time = $stop_time - $start_time; # Время выполнения
	$delta_time = sprintf "%.2f", $delta_time; # Округление
	syslog('info', "Stop parsing file $file: count_rows:$count_rows, count_message_rows:$count_message_rows, count_log_rows:$count_log_rows, count_invalid_rows:$count_invalid_rows, parsing_time:$delta_time sec");
	print "Stop parsing file $file: count_rows:$count_rows, count_message_rows:$count_message_rows, count_log_rows:$count_log_rows, count_invalid_rows:$count_invalid_rows, parsing_time:$delta_time sec\n";
	return;
}

sub chk_int_id
{
	# Есть логи, в которых отсутствует int_id
	# Пример: 2012-02-13 15:08:10 SMTP connection from rtmail.rushost.ru [109.70.26.4] closed by QUIT
	# В данный момент они не вставляются в таблицу log, ибо не имеют валидного int_id
	# (хотя данное поведение можно изменить - скажем для таких логов сделать int_id: 000000-000000-00 и вставлять все такие логи...
	# но, тогда контекст этих таблиц теряется, ибо: int_id CHAR(16) NOT NULL,)
	# В данный момент во время парсинга - эти логи просто записываются в syslog
	my $int_id = shift;
	my $row = shift;
	my $int_id_len = length($int_id);
	if ($int_id_len != 16)
	{
		syslog('info', "WARNING: Invalid int_id: $row");
		$count_invalid_rows++;
		return 0;
	}
	return 1;
}

sub insert_into_message
{
	# Записать в таблицу message
	my $created = shift;
	my $id = shift;
	my $int_id = shift;
	my $str = shift;

	my $rv = $sth_m->execute($created, $id, $int_id, $str);
        db_err($sth_m->errstr) if ($rv == -1);
	$count_message_rows++;
	return;
}

sub insert_into_log_table
{
	# Записать в таблицу log
	my $created = shift;
	my $int_id = shift;
	my $str = shift;
	my $address = shift;

	my $rv = $sth_l->execute($created, $int_id, $str, $address);
	db_err($sth_l->errstr) if ($rv == -1);
	$count_log_rows++;
	return;
}

sub get_script_name
{
        return 'log_parsing';
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

sub release_resources
{
        # Отключение
        $dbh->disconnect() if $dbh;
	return;
}

sub monotime
{
        # Время, независящееся от системного
        return Time::HiRes::clock_gettime(CLOCK_MONOTONIC);
}

sub tables_is_not_empty
{
	# Проверяет наличие строк в таблицах message и log
	my $sql = 'SELECT sum(x.c) from (SELECT count(1) c FROM message union SELECT count(1) c FROM log)x';
	my ($count) = dbh()->selectrow_array($sql);
	if ($count)
	{
		syslog('info', "ERORR! Tables if not empty. TRUNCATE tables message and log before parsing.");
		print "ERORR! Tables if not empty. TRUNCATE tables message and log before parsing.\n";
	}
	return $count;
}
