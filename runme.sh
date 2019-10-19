#cargo run --release --bin rtcd -- -w 1
mode="--release"
#app=toy
app=twitter

if [ "${app}" = "toy" ]; then
  graph=edge:toy_graph.adj.edge
  num_edge_static=5
  timespans='2:8000,6000'
  update_rate=2
  num_workers=30
fi

if [ "${app}" = "twitter" ]; then
  graph=edge:~/datasets/twitter.edge
  num_edge_static=350000
  timespans='2:172800000,86400000'
  update_rate=15000
  num_workers=30
fi

echo $1
if [ "$1" = "debug" ] || [ "$1" = "build" ]; then
  mode=
fi

mkdir logs 2> /dev/null
rm latest.log 2> /dev/null
log_datetime=$(date | awk '{print $2$3"_"$4"_"$6}')
touch ./logs/${log_datetime}.log
cp -s ./logs/${log_datetime}.log latest.log

RUST_BACKTRACE=1 cargo run ${mode} --bin rtcd --\
 ${graph}\
 ${num_edge_static}\
 ${timespans}\
 ${update_rate}\
 -w ${num_workers}\
 | tee -a latest.log
