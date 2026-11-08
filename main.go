package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Result struct {
	Port    int
	Status  string
	Latency time.Duration
}

func parsePorts(input string) ([]int, error) {
	var ports []int

	for _, part := range strings.Split(input, ",") {
		part = strings.TrimSpace(part)

		if part == "" {
			return nil, fmt.Errorf("empty port")
		}

		if strings.Contains(part, "-") {
			bounds := strings.Split(part, "-")

			if len(bounds) != 2 {
				return nil, fmt.Errorf("invalid range: %s", part)
			}

			start, err1 := strconv.Atoi(bounds[0])
			end, err2 := strconv.Atoi(bounds[1])

			if err1 != nil || err2 != nil ||
				start < 1 || end > 65535 || start > end {
				return nil, fmt.Errorf("invalid range: %s", part)
			}

			for p := start; p <= end; p++ {
				ports = append(ports, p)
			}
		} else {
			port, err := strconv.Atoi(part)

			if err != nil || port < 1 || port > 65535 {
				return nil, fmt.Errorf("invalid port: %s", part)
			}

			ports = append(ports, port)
		}
	}

	seen := make(map[int]bool)
	unique := make([]int, 0, len(ports))

	for _, p := range ports {
		if !seen[p] {
			seen[p] = true
			unique = append(unique, p)
		}
	}

	return unique, nil
}

func scanPort(target string, port int, timeout time.Duration) Result {
	address := net.JoinHostPort(
		target,
		strconv.Itoa(port),
	)

	start := time.Now()

	conn, err := net.DialTimeout(
		"tcp",
		address,
		timeout,
	)

	latency := time.Since(start)

	if err == nil {
		_ = conn.Close()

		return Result{
			Port:    port,
			Status:  "OPEN",
			Latency: latency,
		}
	}

	if errors.Is(err, syscall.ECONNREFUSED) {
		return Result{
			Port:    port,
			Status:  "CLOSED",
			Latency: latency,
		}
	}

	var opErr *net.OpError

	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return Result{
				Port:    port,
				Status:  "CLOSED",
				Latency: latency,
			}
		}
	}

	return Result{
		Port:    port,
		Status:  "FILTERED",
		Latency: latency,
	}
}

func worker(
	target string,
	timeout time.Duration,
	jobs <-chan int,
	results chan<- Result,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for port := range jobs {
		results <- scanPort(target, port, timeout)
	}
}

func main() {
	target := flag.String(
		"target",
		"127.0.0.1",
		"Target IP or hostname",
	)

	portInput := flag.String(
		"ports",
		"22,80,443,8080",
		"Ports to scan",
	)

	workers := flag.Int(
		"workers",
		100,
		"Number of concurrent workers",
	)

	timeout := flag.Duration(
		"timeout",
		2*time.Second,
		"Connection timeout",
	)

	flag.Parse()

	if *workers < 1 || *workers > 1000 {
		fmt.Println("Error: workers must be between 1 and 1000")
		os.Exit(1)
	}

	if *timeout <= 0 {
		fmt.Println("Error: timeout must be greater than zero")
		os.Exit(1)
	}

	ports, err := parsePorts(*portInput)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	if len(ports) == 0 {
		fmt.Println("Error: no ports specified")
		os.Exit(1)
	}

	if _, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(*target, "1")); err != nil {
		fmt.Println("Error: invalid target:", err)
		os.Exit(1)
	}

	fmt.Printf("\n")
	fmt.Println("       SYNx - Go Network Scanner")
	fmt.Println("--------------------------------")
	fmt.Printf("Target:  %s\n", *target)
	fmt.Printf("Ports:   %d\n", len(ports))
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Timeout: %s\n", *timeout)
	fmt.Println("--------------------------------")

	jobs := make(chan int, len(ports))
	results := make(chan Result, len(ports))

	var wg sync.WaitGroup

	workerCount := *workers

	if workerCount > len(ports) {
		workerCount = len(ports)
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go worker(
			*target,
			*timeout,
			jobs,
			results,
			&wg,
		)
	}

	for _, port := range ports {
		jobs <- port
	}

	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []Result

	for result := range results {
		allResults = append(allResults, result)
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Port < allResults[j].Port
	})

	var openCount, closedCount, filteredCount int

	fmt.Println()
	fmt.Printf("%-10s %-15s %-12s\n", "PORT", "STATUS", "LATENCY")
	fmt.Println("----------------------------------------")

	for _, result := range allResults {
		fmt.Printf(
			"%-10d %-15s %-12s\n",
			result.Port,
			result.Status,
			result.Latency.Round(time.Millisecond),
		)

		switch result.Status {
		case "OPEN":
			openCount++
		case "CLOSED":
			closedCount++
		case "FILTERED":
			filteredCount++
		}
	}

	fmt.Println("----------------------------------------")
	fmt.Printf("Total:     %d\n", len(allResults))
	fmt.Printf("Open:      %d\n", openCount)
	fmt.Printf("Closed:    %d\n", closedCount)
	fmt.Printf("Filtered:  %d\n", filteredCount)
	fmt.Println("--------------------------------")
}
