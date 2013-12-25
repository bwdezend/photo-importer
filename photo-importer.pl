#!/usr/bin/env perl

# Written by Breandan Dezendorf <breandan@dezendorf.com>
# Released under the GPLv2.

# https://github.com/bwdezend/photo-importer

use warnings;
use strict;

use File::Find;
use File::Basename;
use File::Copy;
use DBI;
use DBD::SQLite;
use Digest::SHA;
use Getopt::Long;

use Image::ExifTool;
use File::Path;
use File::Copy;

my $verbose     = undef;
my $debug       = undef;
my $help        = undef;
my $directory   = undef;
my $reset       = undef;
my $storage_dir = "/Volumes/photos/MasterImages";
my $logfile     = $storage_dir . "/photo-importer.log";

&Getopt::Long::Configure( 'pass_through', 'no_autoabbrev' );
&Getopt::Long::GetOptions(
    'verbose|v'   => \$verbose,
    'debug'       => \$debug,
    'help|h'      => \$help,
    'directory=s' => \$directory,
    'reset'       => \$reset,
    'logfile'     => \$logfile,
);

if ( $logfile eq "none" ) { $logfile = undef }

# If you want to import from PhotoStream, set $directory to:
#
# $ENV{"HOME"} . '/Library/Application Support/iLifeAssetManagement/assets/sub';
#
# We're importing from the Dropbox camera uploads by default instead:

if ( !$directory ) { $directory = $ENV{"HOME"} . '/Dropbox/Camera Uploads'; }

my $db_file = $ENV{"HOME"} . '/homefolder/etc/processed.sqlite3';

if ($help) {
    print "photo-importer\n";
    print "  A utility to scan a given directory looking for image files\n";
    print "   and copy new files to a given storage directory. This is done\n";
    print "   using SHA1 hashes of the files. The files are stored based on\n";
    print
      "   EXIF date data (storage/YYYY/YYYY-MM/YYYY-MM-DD/image_file.jpg)\n";
    print "\n";
    print "  --help\n";
    print "       prints this help message\n";
    print "  --directory <path to directory to process for duplicates>\n";
    print "       default: '$directory'\n";
    print "  --verbose\n";
    print "       prints more messages about what's going on, what's being\n";
    print "       copied and what's not.\n";
    print "  --debug\n";
    print
      "       prints lots of extra messages about what the program is doing\n";
    print "  --reset\n";
    print "       deletes all data in sqlite db files\n";
    print "       ($db_file)\n";
    exit 0;
}
&log_message("Starting run");
$SIG{'INT'} = sub {
    &log_message("Time for an orderly shutdown");
    exit 0;
};

my %digests = ();

my $dbh = DBI->connect( "dbi:SQLite:dbname=$db_file", "", "" );

if ($reset) {
    $dbh->do("DROP TABLE IF EXISTS hashes");
    $dbh->do("CREATE TABLE hashes(Id INT PRIMARY KEY, Name TEXT, Hash TEXT)");
    exit 0;
}

# Pull all the saved hashes from the sqlite db
my $sth = $dbh->prepare("SELECT * FROM hashes");
$sth->execute();

my $results = $sth->fetchall_hashref('Hash');

print "Searching $directory for duplicate files based on SHA1 hashes\n"
  if ($verbose);

my $copy_count = 0;
File::Find::find( \&wanted, $directory );

if ( $copy_count gt 0 ) {
    &terminal_notify($copy_count);
}

&log_message("Run finished");

exit 0;

sub wanted {
    my $digest   = undef;
    my $sha      = Digest::SHA->new('SHA1');
    my $exifTool = new Image::ExifTool;

    if ( -f $_ ) {
        $sha->addfile($_);
        $digest = $sha->hexdigest;

        my $basename = basename($File::Find::name);
        if ( $results->{$digest} ) {
            &log_message("exists:  $digest - $basename") if ($debug);
        }
        else {
            my $dest_dir = undef;
            &log_message("unique:  $digest - $basename") if ($verbose);
            $sth =
              $dbh->prepare("INSERT INTO hashes(Name, Hash) VALUES (?, ?)");
            $sth->execute( $basename, $digest );

            my $info = $exifTool->ImageInfo($_);

            if ( defined $info->{'DateTimeOriginal'}
                && $info->{'DateTimeOriginal'} =~
                m/^(\d\d\d\d):(\d\d):(\d\d) / )
            {
                my $year  = $1;
                my $date  = $3;
                my $month = $2;
                $date =~ s/:/-/g;
                $dest_dir =
                    $storage_dir . "/"
                  . $year . "/"
                  . $year . "-"
                  . $month . "/"
                  . $year . "-"
                  . $month . "-"
                  . $date;
                print "storage_dir: $dest_dir\n" if ($debug);
                unless ( -d "$dest_dir" ) {
                    mkpath($dest_dir);
                    print "  created $dest_dir\n" if ($verbose);
                }
            }
            if ($dest_dir) {
                my $image_file = $dest_dir . '/' . $basename;
                if ( -e $image_file ) {
                    $sha->addfile($image_file);
                    my $exists = $sha->hexdigest;

                    &log_message("nocopy:  $digest - $basename");
                    &log_message("nocopy:  $exists - $image_file");
                }
                else {
                    &log_message("copy:    $digest - $basename");

                    #print "copy:    $digest - $basename\n";
                    copy $_, $image_file or die $!;
                    $copy_count++;
                }
            }
            else {
                print
"skipping $basename: unable to get EXIF -- DateTimeOriginal\n";
            }
        }
        $digests{$digest} = $File::Find::name;

    }
}

sub log_message {

    my $message = shift;
    chomp $message;
    my $time = time();
    print "$message\n" if ( $verbose || $debug );
    if ($logfile) {
        open( LOG, ">> $logfile" );
        printf LOG ( $time . ": " . $message . "\n" );
    }
}

sub terminal_notify {
    my $count = shift;
    if ( -x "/usr/local/bin/terminal-notifier2" ) {
        my $NOTIFY =
'/usr/local/bin/terminal-notifier -title "photo-importer" -message "'
          . $count
          . ' new messages added to photo vault"';
        my $result = `$NOTIFY`;
    }

}


