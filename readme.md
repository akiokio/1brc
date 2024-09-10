# Results
Best so far 10s (wall clock)
```
1brc on  main via  v1.23.0
❯ go build -o 1brc && time ./1brc data/measurements-1000000000.txt
2024/09/09 23:23:55 Reading file: data/measurements-1000000000.txt
2024/09/09 23:24:06 Done
./1brc data/measurements-1000000000.txt  88.78s user 2.43s system 866% cpu 10.525 total

1brc on  main via  v1.23.0 took 10s
```
