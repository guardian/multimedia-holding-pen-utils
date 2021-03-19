package main

import "testing"

func TestPrefixFromFilename(t *testing.T) {
	firstResult, firstFound := prefixFromFilename("path/to/some_file.mxf")
	if !firstFound {
		t.Error("nothing found for path/to/some_file.mxf")
	}
	if firstResult != "path/to/some_file" {
		t.Errorf("got incorrect prefix '%s' on first test", firstResult)
	}

	secondResult, secondFound := prefixFromFilename("path/to/some.file.with.dots.mc2")
	if !secondFound {
		t.Error("nothing found for second test")
	}
	if secondResult != "path/to/some.file.with.dots" {
		t.Errorf("got incorrect prefix '%s' on second test", secondResult)
	}

	thirdResult, thirdFound := prefixFromFilename("path/to/some_file")
	if thirdFound {
		t.Error("something found for third test")
	}
	if thirdResult != "path/to/some_file" {
		t.Errorf("got incorrect prefix '%s' on third test", thirdResult)
	}
}
