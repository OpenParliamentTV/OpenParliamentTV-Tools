<?php


// Check if the script is already running
$alreadyRunning = shell_exec("ps aux | grep -v grep | grep basescript | wc -l");

if ($alreadyRunning > 2) {
	echo "basescript already running. Exiting.\n";
	file_put_contents(__DIR__."/ret.log",date("Y-m-d H:i:s")." basescript not started because its already running.\n",FILE_APPEND);
	exit;
} else {
	file_put_contents(__DIR__."/ret.log",date("Y-m-d H:i:s")." basescript started.\n",FILE_APPEND);
	echo "basescript started.\n";
}

// Navigate to the directory
chdir("/var/www/html/OpenParliamentTV-Parsers/parliaments/DE");
//exec("make download", $returns);

function makeDownload() {
	$returns = array();
	//exec("make download 2>&1", $returns);
	//exec("python3 update_and_merge.py --save-raw-data --from-period=19 --second-stage-matching data 2>&1", $returns);
	exec("python3 update_and_merge.py --save-raw-data --from-period=20 --second-stage-matching --advanced-rematch data 2>&1", $returns);

	$loop = false;
	foreach ($returns as $ret) {
		file_put_contents(__DIR__."/ret.log",date("Y-m-d H:i:s")." ".$ret."\n",FILE_APPEND);
		if (preg_match("~error \(503\)~",$ret)) {
			$loop = true;
		}


	}
	if ($loop === true) {
		makeDownload();
	}

}

makeDownload();

/*
 * deprecated
function make() {
        $returns = array();
        exec("make 2>&1", $returns);

        $loop = false;
        foreach ($returns as $ret) {
                file_put_contents(__DIR__."/ret.log",$ret."\n",FILE_APPEND);
        }
}


make();



function makeMerge() {
        $returns = array();
        exec("make forcemerge 2>&1", $returns);

        $loop = false;
        foreach ($returns as $ret) {
                file_put_contents(__DIR__."/ret.log",$ret."\n",FILE_APPEND);
        }
}


makeMerge();
*/

chdir("/var/www/html/OpenParliamentTV-Alignment/");

exec ("php /var/www/html/OpenParliamentTV-Alignment/index_local.php");

/* for writing the output of the index_local to basescript's log:

while (@ ob_end_flush()); // end all output buffers if any

$proc = popen("php /var/www/html/OpenParliamentTV-Alignment/index_local.php", 'r');

while (!feof($proc))
{
	file_put_contents(__DIR__."/ret.log",date("Y-m-d H:i:s")." ".fread($proc, 4096)."",FILE_APPEND);
	@ flush();
}
*/
?>
