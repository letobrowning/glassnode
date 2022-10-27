I've had a great pleasure making this solution, so I've made some kind of MVP and it took approximately three days. It works, but further tests, corrections, and optimizations are inevitably required. Normally it takes a week at least to make a stable, production-ready microservice like that. In normal circumstances, of course, I would discuss all my decisions in advance. Meanwhile I took my chance to demonstrate some of my skills I use on a daily basis in building highload bigdata services. All of that is because I treat every product I create as a 'real-life' solution and I can not provide you with a code that would inevitably break on a bigger database or load. It's obviously a rabbit hole in having a REST service because it means unlimited parallel querying unless it is explicitly limited in specs. Otherwise, I expect it to be as hard as possible. It makes me to create various load-balancing, throttling, and parallelization solutions I've made and described. Please read the commentaries in the source files so they continue this document.

1) puller.go - internal microservice (please start from here)
2) solution.go - external microservice
3) peakload.go - peak loading test
4) extend.go - database extension for tests

Machine:
    10 cores of AMD EPYC 7282
    50 GB RAM

How to test it:

 > docker-compose up
 > curl 'http://127.0.0.1:7001/?from=1599436800&to=1599523200' # single query
 > ./peakload # peak load test

Peak tests:

1) On the given 1 day DB (340 Mb)
    2022/10/16 15:39:45 Threads: 5
    2022/10/16 15:39:45 Transactions from '2020-09-07 00:00:04' to '2020-09-07 23:59:54'
    2022/10/16 15:39:55 34237/34237 (100.00%) correct of total 34242.00 requests, speed: 3423.70/sec, avg. speed: 3423.70/sec
    2022/10/16 15:40:05 62067/62067 (100.00%) correct of total 62072.00 requests, speed: 2783.00/sec, avg. speed: 3103.35/sec
    2022/10/16 15:40:15 96870/96870 (100.00%) correct of total 96874.00 requests, speed: 3480.30/sec, avg. speed: 3229.00/sec
    2022/10/16 15:40:25 130313/130313 (100.00%) correct of total 130316.00 requests, speed: 3344.30/sec, avg. speed: 3257.82/sec
    2022/10/16 15:40:35 143487/143487 (100.00%) correct of total 143490.00 requests, speed: 1317.40/sec, avg. speed: 2869.74/sec
    2022/10/16 15:40:45 151064/151064 (100.00%) correct of total 151069.00 requests, speed: 757.70/sec, avg. speed: 2517.73/sec
    2022/10/16 15:40:55 179111/179111 (100.00%) correct of total 179116.00 requests, speed: 2804.70/sec, avg. speed: 2558.73/sec
    2022/10/16 15:41:05 210052/210052 (100.00%) correct of total 210057.00 requests, speed: 3094.10/sec, avg. speed: 2625.65/sec
    2022/10/16 15:41:15 237861/237861 (100.00%) correct of total 237865.00 requests, speed: 2780.90/sec, avg. speed: 2642.90/sec
    2022/10/16 15:41:25 272991/272991 (100.00%) correct of total 272995.00 requests, speed: 3513.00/sec, avg. speed: 2729.91/sec
    2022/10/16 15:41:35 292790/292790 (100.00%) correct of total 292793.00 requests, speed: 1979.90/sec, avg. speed: 2661.73/sec
    2022/10/16 15:41:45 300234/300234 (100.00%) correct of total 300238.00 requests, speed: 744.40/sec, avg. speed: 2501.95/sec
    2022/10/16 15:41:55 322525/322525 (100.00%) correct of total 322530.00 requests, speed: 2229.10/sec, avg. speed: 2480.96/sec
    2022/10/16 15:42:05 353126/353126 (100.00%) correct of total 353130.00 requests, speed: 3060.10/sec, avg. speed: 2522.33/sec
    2022/10/16 15:42:15 377711/377711 (100.00%) correct of total 377716.00 requests, speed: 2458.50/sec, avg. speed: 2518.07/sec
    2022/10/16 15:42:25 403959/403959 (100.00%) correct of total 403963.00 requests, speed: 2624.80/sec, avg. speed: 2524.74/sec
    2022/10/16 15:42:35 436437/436437 (100.00%) correct of total 436442.00 requests, speed: 3247.80/sec, avg. speed: 2567.28/sec
    2022/10/16 15:42:45 443657/443657 (100.00%) correct of total 443662.00 requests, speed: 722.00/sec, avg. speed: 2464.76/sec
    2022/10/16 15:42:55 460666/460666 (100.00%) correct of total 460671.00 requests, speed: 1700.90/sec, avg. speed: 2424.56/sec
    2022/10/16 15:43:05 488582/488582 (100.00%) correct of total 488586.00 requests, speed: 2791.60/sec, avg. speed: 2442.91/sec
    2022/10/16 15:43:15 521354/521354 (100.00%) correct of total 521357.00 requests, speed: 3277.20/sec, avg. speed: 2482.64/sec
    2022/10/16 15:43:25 547540/547540 (100.00%) correct of total 547545.00 requests, speed: 2618.60/sec, avg. speed: 2488.82/sec
    2022/10/16 15:43:35 574224/574224 (100.00%) correct of total 574227.00 requests, speed: 2668.40/sec, avg. speed: 2496.63/sec
    2022/10/16 15:43:45 589087/589087 (100.00%) correct of total 589092.00 requests, speed: 1486.30/sec, avg. speed: 2454.53/sec
    2022/10/16 15:43:55 601732/601732 (100.00%) correct of total 601736.00 requests, speed: 1264.50/sec, avg. speed: 2406.93/sec
    2022/10/16 15:44:05 629073/629073 (100.00%) correct of total 629077.00 requests, speed: 2734.10/sec, avg. speed: 2419.51/sec
    2022/10/16 15:44:15 660135/660135 (100.00%) correct of total 660139.00 requests, speed: 3106.20/sec, avg. speed: 2444.94/sec
    2022/10/16 15:44:25 684023/684023 (100.00%) correct of total 684028.00 requests, speed: 2388.80/sec, avg. speed: 2442.94/sec
    2022/10/16 15:44:35 711679/711679 (100.00%) correct of total 711684.00 requests, speed: 2765.60/sec, avg. speed: 2454.07/sec
    2022/10/16 15:44:45 729468/729468 (100.00%) correct of total 729470.00 requests, speed: 1778.90/sec, avg. speed: 2431.56/sec
    2022/10/16 15:44:55 741256/741256 (100.00%) correct of total 741260.00 requests, speed: 1178.80/sec, avg. speed: 2391.15/sec
    2022/10/16 15:45:05 764634/764634 (100.00%) correct of total 764639.00 requests, speed: 2337.80/sec, avg. speed: 2389.48/sec
    2022/10/16 15:45:15 794907/794907 (100.00%) correct of total 794912.00 requests, speed: 3027.30/sec, avg. speed: 2408.81/sec
    2022/10/16 15:45:25 822443/822443 (100.00%) correct of total 822447.00 requests, speed: 2753.60/sec, avg. speed: 2418.95/sec
    2022/10/16 15:45:35 846747/846747 (100.00%) correct of total 846751.00 requests, speed: 2430.40/sec, avg. speed: 2419.28/sec
    2022/10/16 15:45:45 868641/868641 (100.00%) correct of total 868645.00 requests, speed: 2189.40/sec, avg. speed: 2412.89/sec
    2022/10/16 15:45:55 877939/877939 (100.00%) correct of total 877943.00 requests, speed: 929.80/sec, avg. speed: 2372.81/sec
    2022/10/16 15:46:05 901614/901614 (100.00%) correct of total 901619.00 requests, speed: 2367.50/sec, avg. speed: 2372.67/sec
    2022/10/16 15:46:15 926519/926519 (100.00%) correct of total 926523.00 requests, speed: 2490.50/sec, avg. speed: 2375.69/sec
    2022/10/16 15:46:25 957402/957402 (100.00%) correct of total 957407.00 requests, speed: 3088.30/sec, avg. speed: 2393.51/sec
    2022/10/16 15:46:35 979245/979245 (100.00%) correct of total 979249.00 requests, speed: 2184.30/sec, avg. speed: 2388.40/sec
    2022/10/16 15:46:45 1004830/1004830 (100.00%) correct of total 1004833.00 requests, speed: 2558.50/sec, avg. speed: 2392.45/sec
    2022/10/16 15:46:55 1012616/1012616 (100.00%) correct of total 1012621.00 requests, speed: 778.60/sec, avg. speed: 2354.92/sec
    2022/10/16 15:47:05 1032080/1032080 (100.00%) correct of total 1032085.00 requests, speed: 1946.40/sec, avg. speed: 2345.64/sec
    2022/10/16 15:47:15 1062903/1062903 (100.00%) correct of total 1062908.00 requests, speed: 3082.30/sec, avg. speed: 2362.01/sec
    2022/10/16 15:47:25 1093419/1093419 (100.00%) correct of total 1093423.00 requests, speed: 3051.60/sec, avg. speed: 2377.00/sec
    2022/10/16 15:47:35 1120447/1120447 (100.00%) correct of total 1120452.00 requests, speed: 2702.80/sec, avg. speed: 2383.93/sec
    2022/10/16 15:47:45 1144279/1144279 (100.00%) correct of total 1144283.00 requests, speed: 2383.20/sec, avg. speed: 2383.91/sec
    2022/10/16 15:47:55 1154073/1154073 (100.00%) correct of total 1154077.00 requests, speed: 979.40/sec, avg. speed: 2355.25/sec
    2022/10/16 15:48:05 1171649/1171649 (100.00%) correct of total 1171654.00 requests, speed: 1757.60/sec, avg. speed: 2343.30/sec
    2022/10/16 15:48:15 1194977/1194977 (100.00%) correct of total 1194982.00 requests, speed: 2332.80/sec, avg. speed: 2343.09/sec
    2022/10/16 15:48:25 1228969/1228969 (100.00%) correct of total 1228973.00 requests, speed: 3399.20/sec, avg. speed: 2363.40/sec

2) ... extending it
