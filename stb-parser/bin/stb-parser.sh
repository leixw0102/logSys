#!/bin/sh
source /etc/profile
source ~/.bashrc
day=$(date +%Y-%m-%d --date '1 days ago')
randTime=$(date +%s)
day1=$(date +%Y%m%d --date '1 days ago')

## -I  hdfs input
## -Back  processing file back directory
## －FKeys
hadoop jar $(cd "$(dirname "$0")"; pwd)/stb-parser-1.0-SNAPSHOT.jar allJob -I /icntv/log/stb/$day/ -Back /icntv/parser/stb/filter/status/$day/  -FOut /icntv/parser/stb/filter/result/$day/ -FKeys '800-userLogin/'$randTime'/userLogin;900-contentView/'$randTime'/contentView;100-devicePlayer/'$randTime'/devicePlayer;101,102,103,104,105,106,107-logEpg/'$randTime'/logEpg;108-replay/'$randTime'/replay;401-cdn/'$randTime'/cdn;402-cdnAdapter/'$randTime'/cdnAdapter' -FOther other/$randTime/other -PMInput /icntv/parser/stb/filter/result/$day/devicePlayer/$randTime -PMOut /icntv/parser/stb/devicePlayer/middle/$day/ -UMInput /icntv/parser/stb/filter/result/$day/userLogin/$randTime -UMOut /icntv/parser/stb/userLogin/middle/$day/ -CMInput /icntv/parser/stb/filter/result/$day/contentView/$randTime -CMOut /icntv/parser/stb/contentView/middle/$day/ -LBMInput /icntv/parser/stb/filter/result/$day/replay/$randTime  -LBMOut /icntv/parser/stb/replay/middle/$day/ -LEMInput /icntv/parser/stb/filter/result/$day/logEpg/$randTime -LEMOut /icntv/parser/stb/logEpg/middle/$day/ -CDNMInput /icntv/parser/stb/filter/result/$day/cdn/$randTime -CDNMOut /icntv/parser/stb/cdn/middle/$day/
####execute generate dat######



hadoop jar $(cd "$(dirname "$0")"; pwd)/tools-1.0-SNAPSHOT.jar -I /icntv/parser/stb/contentView/middle/$day/ -Out /icntv/parser/stb/contentView/result/$day/contentviewlog_$day1.dat
hadoop jar $(cd "$(dirname "$0")"; pwd)/tools-1.0-SNAPSHOT.jar -I /icntv/parser/stb/userLogin/middle/$day/ -Out /icntv/parser/stb/userLogin/result/$day/userLoginInfo_$day1.dat
hadoop jar $(cd "$(dirname "$0")"; pwd)/tools-1.0-SNAPSHOT.jar -I /icntv/parser/stb/devicePlayer/middle/$day/ -Out /icntv/parser/stb/devicePlayer/result/$day/deviceplayer_$day1.dat
hadoop jar $(cd "$(dirname "$0")"; pwd)/tools-1.0-SNAPSHOT.jar -I /icntv/parser/stb/replay/middle/$day/ -Out /icntv/parser/stb/replay/result/$day/replay_$day1.dat
hadoop jar $(cd "$(dirname "$0")"; pwd)/tools-1.0-SNAPSHOT.jar -I /icntv/parser/stb/logEpg/middle/$day/ -Out /icntv/parser/stb/logEpg/result/$day/useroperepginfo_$day1.dat
hadoop jar $(cd "$(dirname "$0")"; pwd)/tools-1.0-SNAPSHOT.jar -I /icntv/parser/stb/cdn/middle/$day/ -Out /icntv/parser/stb/cdn/result/$day/cdnloginfo_$day1.dat
