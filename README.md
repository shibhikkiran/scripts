# scripts

```
$ python3 ./hit_stats.py --help
usage: hit_stats.py [-h] [-f FILES] [-u] [-a]

optional arguments:
  -h, --help            show this help message and exit
  -f FILES, --files FILES
                        log files(text, .zip, .bz2, gzip) to be extracted
                        seperated by comma(file1.log,file2.log,filen.log)
  -u, --url-hits        stats on unique url hits
  -a, --api-hits        stats on unique api hits
  ```
  
  ```
  #1. Dump URL hit stats from multiple files and ordered by date:
  $ python3 ./hit_stats.py --files /var/log/access1.log,/var/log/access2.gzip --url-hits
  
 ```
