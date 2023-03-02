package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	res := 0
	for num := range nums{
		res += num
	}

	out <- res	
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	f, err := os.Open(fileName)
	checkError(err)
	defer f.Close()
    
	numbers, err := readInts(f)
    
	seg := len(numbers) / num

	if err != nil {
		panic(fmt.Sprintf("error in file : %s", fileName))
	}

	result := 0
	sumch := make(chan int, num)
	for i := 0; i < num; i++ {
		start, end := i * seg, (i + 1) * seg
		if i == num -1 {
			if end >= len(numbers) {
				end = len(numbers)
			}
		}

		ch := make(chan int, end - start)
		go sumWorker(ch, sumch)

		go func(start, end int, ch chan int){
			for i := start; i < end; i++ {
				ch <- numbers[i]
			}
			
			close(ch)
		}(start, end, ch)
	}

	for i := 0; i < num; i++ {
		result += <-sumch
	}

	close(sumch)
	return result

	return 0
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
