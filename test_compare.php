#!/usr/bin/php
<?php

/*
A script to compare results between two PetScan installations, via random PSIDs.
*/

#$one_psid = 8555023 ; // TESTING
$verbose = isset($one_psid); 

function compare_values($jo,$jn) {
	global $verbose;
	if ( is_object($jo) && is_object($jn) ) {
		$ko = array_keys((array)$jo) ;
		$kn = array_keys((array)$jn) ;
		sort($ko);
		sort($kn);
		if ( $ko == $kn ) {
			$ret = true ;
			foreach ( $kn AS $key ) {
				if ( $key == "querytime_sec" ) continue ;
				if ( $key == "query" ) continue ;
				if ( $key == "nstext" ) continue ; // New version is actually correct, old isn't always
				if ( !compare_values($jo->$key,$jn->$key) ) {
					$ret = false ;
				}
			}
			return $ret;
		} else {
			if ( $verbose ) print "1Different:\n".json_encode($jo)."\nand\n".json_encode($jn)."\n\n" ;
		}
	} else if ( is_array($jo) && is_array($jn) ) {
		if ( count($jo) == count($jn) ) {
			$ret = true ;
			for ( $k = 0 ; $k < count($jn) ; $k++ ) {
				if ( !compare_values($jo[$k],$jn[$k]) ) {
					$ret = false ;
				}
			}
			return $ret ;
		} else {
			if ( $verbose ) print "2Different:\n".json_encode($jo)."\nand\n".json_encode($jn)."\n\n" ;
		}
	} else {
		if ( $jo == $jn ) {
			return true ;
		} else {
			if ( $verbose ) print "3Different:\n".json_encode($jo)."\nand\n".json_encode($jn)."\n\n" ;
		}
	}
	return false;
}

function compare_results($psid) {
	$url_orig = "https://petscan1.wmflabs.org/?psid={$psid}&format=json";
	$url_new = "https://petscan.wmflabs.org/?psid={$psid}&format=json";
	//print "{$url_orig} | {$url_new}\n" ;
	$jo = json_decode(file_get_contents($url_orig));
	$jn = json_decode(file_get_contents($url_new));
	if ( $jo === null ) {
		print "_" ;
		return;
	}
	if ( isset($jn->error) ) {
		print "#" ;
		return ;
	}
	foreach ( ['a','querytime_sec','query'] AS $key ) {
		unset($jo->$key ) ;
		unset($jn->$key ) ;
	}
	if ( !compare_values($jo,$jn) ) {
		print "\nMISMATCH:\n{$url_orig} | {$url_new}\n" ;
		exit(0);
	}
	print ".";
}

for ( $k = 0 ; $k < 100 ; $k++ ) {
	if (isset($one_psid)) $psid = $one_psid;
	else $psid = rand(0,10651180);
	compare_results($psid);
	if (isset($one_psid)) exit(0);
}

?>