#!/usr/bin/env perl

# Written by Breandan Dezendorf <breandan@dezendorf.com>
# Released under the GPL.

use warnings;
use strict;

use File::Find;
use File::Basename;
use File::Copy;
use DBI;
use DBD::SQLite;
use Digest::SHA;
use Getopt::Long;
use POSIX qw(strftime);

use Time::localtime;

use Image::ExifTool;
use File::Path;
use File::Copy;

my $verbose     = undef;
my $debug       = undef;
my $help        = undef;
my $directory   = undef;
my $reset       = undef;
my $storage_dir = "/mnt/nfs/photos/MasterImages";
my $logfile     = $storage_dir . "/photostream2lightroom.log";
my $lockfile    = "/tmp/photo-import.lock";
my $sleep       = 1;
my $delete      = undef;
my $no_import   = undef;
my $icloud      = 1;
my $reimport    = undef;
my $unlink      = undef;

my $year = strftime "%Y", localtime;
my $month = strftime "%m", localtime;
my $day = strftime "%d", localtime;



&Getopt::Long::Configure( 'pass_through', 'no_autoabbrev' );
&Getopt::Long::GetOptions(
    'verbose|v'   => \$verbose,
    'debug'       => \$debug,
    'help|h'      => \$help,
    'directory=s' => \$directory,
    'reset'       => \$reset,
    'logfile'     => \$logfile,
    'sleep=i'     => \$sleep,
    'delete'      => \$delete,
    'no_import'   => \$no_import,
    'icloud'      => \$icloud,
    'reimport'    => \$reimport,
    'unlink'      => \$unlink,
);

if ( $logfile eq "none" ) { $logfile = undef }

if ( !$directory && $icloud ) { $directory = $ENV{"HOME"} . '/Pictures/Photos Library.photoslibrary/Masters/' . $year . '/' . $month . '/'; }
if ( !$directory ) { $directory = $ENV{"HOME"} . '/Dropbox/Camera Uploads'; }

if ( !$sleep ) { $sleep = '0' }

my $db_file = $ENV{"HOME"} . '/homefolder/etc/processed.sqlite3';

if ($help) {
    print "finddupes\n";
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
    if ($debug) { print " Removing lockfile ($lockfile)\n" }
    system("rm $lockfile");
    exit 0;
};

my %digests = ();

my $dbh = DBI->connect( "dbi:SQLite:dbname=$db_file", "", "" );

if (-e $lockfile){ die "Could not lock $lockfile\n" } else { system("touch $lockfile") };

unless (-d $storage_dir){
	print "$storage_dir doesn't exist. Leaving $lockfile in place!\n";
	system("touch $lockfile");
	exit 1;
}


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

system("rm $lockfile");
&log_message("Run finished");

exit 0;

sub wanted {
    my $digest        = undef;
    my $sha           = Digest::SHA->new('SHA1');
    my $exifTool      = new Image::ExifTool;
    my $should_unlink = undef;

    if ( -f $_ ) {
        $sha->addfile($_);
        $digest = $sha->hexdigest;

        my $basename = basename($File::Find::name);
        if ( !$reimport && $results->{$digest} ) {
            &log_message("exists:  $digest - $basename") if ($debug);
        } elsif ( $basename =~ m/.xmp/ ) {
           print "not processing xmp files\n" if ($debug);
        } else {
            my $dest_dir = undef;
            &log_message("unique:  $digest - $basename") if ($debug);

            my $info = $exifTool->ImageInfo($_);

            if ( defined $info->{'DateTimeOriginal'}
                && $info->{'DateTimeOriginal'} =~
                m/^(\d\d\d\d):(\d\d):(\d\d) / )
            {
                my $year  = $1;
                my $date  = $3;
                my $month = $2;
                if ($year == "0000"){
                   print "ERROR: invalid exif\n";
                } else {
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
                	unless ($no_import) {
	                	$sth = $dbh->prepare("INSERT INTO hashes(Name, Hash) VALUES (?, ?)");
    	            	$sth->execute( $basename, $digest );
    	            }
                }

            }
            if ($dest_dir) {
                my $image_file = $dest_dir . '/' . $basename;
                if ( -e $image_file ) {
                    print "Calculating SHA for $image_file\n" if ($debug);
                    $sha->addfile($image_file);
                    my $exists = $sha->hexdigest;

                    &log_message("nocopy:  $digest - $basename == $image_file");

                 	if ($unlink){
                 	    if ( $exists eq $digest ) {
							print "unlink: $basename \n";
							unlink $basename;
						} else {
						    print "unlink failed:\n    $exists\n    $digest\n don't match\n";
						}
            		}               	

                }
                else {
                    unless ($no_import) {
	                    &log_message("copy:    $digest - $basename into $image_file");
	                    my $source_file = $_;
                    	copy $source_file, $image_file or die $!;

                    	if ($unlink){
		                    print "Calculating SHA for $image_file\n" if ($debug);
        		            $sha->addfile($image_file);
		                    my $exists = $sha->hexdigest;
		                    if ( $exists eq $digest ) {
								print "unlink: $basename \n";
								unlink $basename;
							} else {
						  		print "unlink failed:\n    $exists\n    $digest\n don't match\n";
							}
                		}               	

	                    my $xmp_sidecar_src = $source_file . ".xmp";
	                    my $xmp_sidecar_dest = $image_file . ".xmp";
                    	if ( -e $xmp_sidecar_src) {
                    		&log_message("copy sidecar:  $xmp_sidecar_src - $xmp_sidecar_dest");
                    		copy $xmp_sidecar_src, $xmp_sidecar_dest or die $!;
                    		if ($unlink){
                    		  print "unlink sidecar: $xmp_sidecar_src \n";
                    		  unlink $xmp_sidecar_src;
                    		}               	
                    	}
	                    $copy_count++;
                    }
                }
            }
            else {
                unless ($basename =~ m/.xmp/){
                    print
"skipping $basename: unable to get EXIF -- DateTimeOriginal\n";
				}
            }
        }
        $digests{$digest} = $File::Find::name;
        sleep($sleep);
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
    if ( -x "/usr/local/bin/terminal-notifier" ) {
        my $NOTIFY =
'/usr/local/bin/terminal-notifier -title "photostream2lightroom" -message "'
          . $count
          . ' new messages added to photo vault"';
        my $result = `$NOTIFY`;
    }

}
