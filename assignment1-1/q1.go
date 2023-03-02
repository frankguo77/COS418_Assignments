package cos418_hw1_1

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"
	data, err := ioutil.ReadFile(path)
	checkError(err)
	wordsCounter := make(map[string]int)

	var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

	for _, line := range strings.Split(string(data), "\n") {
		for _, word := range strings.Split(line, " ") {
			word = normalize(word, nonAlphanumericRegex)
			if len(word) == 0 || len(word) < charThreshold {
				continue
			}

			wordsCounter[word] += 1
		}
	}
	//fmt.Println(wordsCounter)

	return getTopKresult(wordsCounter, numWords)
	//return getTopKresultByHeap(wordsCounter, numWords)
}

func getTopKresult(wordsCounter map[string]int, k int) []WordCount {
	res := make([]WordCount, 0)

	for word, count := range wordsCounter {
		res = append(res, WordCount{word, count})
	}

	sortWordCounts(res)

	topKresult := make([]WordCount, 0)

	for i := 0; i < k; i++ {
		topKresult = append(topKresult, res[i])
	}

	return topKresult
}

func getTopKresultByHeap(wordsCounter map[string]int, k int) []WordCount {
	h := &WordCountHeap{}

	for w, c := range wordsCounter {
		wc := WordCount{w, c}
		if len(*h) < k {
			heap.Push(h, wc)
		} else {
			if (*h)[0].Less(wc) {
				heap.Push(h, wc)
				heap.Pop(h)
			}
		}
	}

	// fmt.Println(*h)

	topKresult := make([]WordCount, k)

	for i := len(*h); i > 0; i-- {
		topKresult[i-1] = heap.Pop(h).(WordCount)
	}

	return topKresult
}

func normalize(word string, r *regexp.Regexp) string {
	return strings.ToLower(r.ReplaceAllString(word, ""))
}

// Note the difference between this less and the less in sort.Slice
func (wc WordCount) Less(other WordCount) bool {
	if wc.Count == other.Count {
		return wc.Word > other.Word
	}

	return wc.Count < other.Count
}

type WordCountHeap []WordCount

func (h WordCountHeap) Len() int           { return len(h) }
func (h WordCountHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h WordCountHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *WordCountHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(WordCount))
}

func (h *WordCountHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
