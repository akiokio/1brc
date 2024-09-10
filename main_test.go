package main

import (
	"log"
	"os"
	"strings"
	"testing"
)

func TestCorrectness10(t *testing.T) {
	file, err := os.ReadFile("data/measurements-10.out")
	if err != nil {
		log.Fatal(err)
	}
	expected := string(file)
	// Remove { and } from the expected content
	expected = strings.TrimPrefix(expected, "{")
	expected = strings.TrimSuffix(expected, "}\n")

	result := processFile("data/measurements-10.txt")

	expected = strings.TrimSpace(expected)
	result = strings.TrimSpace(result)

	if result != expected {
		t.Errorf("Files are different.\nExpected: %s\nGot: %s\n", expected, result)
	}

}

func TestCorrectness(t *testing.T) {
	file, err := os.ReadFile("data/measurements-100000.out")
	if err != nil {
		log.Fatal(err)
	}
	expected := string(file)
	// Remove { and } from the expected content
	expected = strings.TrimPrefix(expected, "{")
	expected = strings.TrimSuffix(expected, "}\n")

	result := processFile("data/measurements-100000.txt")

	expected = strings.TrimSpace(expected)
	result = strings.TrimSpace(result)

	if result != expected {
		t.Errorf("Files are different.\nExpected: %s\nGot: %s\n", expected, result)
	}

}
