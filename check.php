#!/usr/bin/php
<?php

error_reporting(E_ERROR|E_CORE_ERROR|E_COMPILE_ERROR|E_ALL);
ini_set('display_errors', 'On');

$seconds = 30 ;

function restart_petscan () {
	$cmd = 'sudo killall petscan_rs' ;
	exec ( $cmd ) ;
	print "Restarted PetScan\n" ;
}

while ( 1 ) {
	$r = rand();
	$url = "http://petscan.wmflabs.org/?psid=16306546&format=csv&killer=1&random={$r}" ;
	$ok = false ;
	try {
		$csv = @file_get_contents($url) ;
		$ok = preg_match ( '|Magnus_Manske|' , $csv ) ;
	} catch ( Exception $e ) {
	}
	if ( !$ok ) restart_petscan() ;
	sleep($seconds);
}

?>