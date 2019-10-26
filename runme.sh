#cargo run --release --bin rtcd -- -w 1
mode="--release"
expr_time=300
app=toy
#app=twitter

if [ "${app}" = "toy" ]; then
  graph=edge:toy_graph.edge
  num_edge_static=5
  num_edge_streaming=$(bc <<< "$(wc -l toy_graph.edge | awk '{print $1}')-${num_edge_static}")
  timespans='2:6#8000,6#6000'
  update_rate=2
  num_workers=2
fi

if [ "${app}" = "twitter" ]; then
  graph=edge:~/datasets/twitter.edge
  num_edge_static=350000
  timespans='2:6#172800000,6#86400000'
  update_rate=3000
  num_edge_streaming=$(bc -l <<< "${expr_time}*${update_rate}")
  num_workers=20
fi

#echo ${num_edge_static} ${num_edge_streaming} ${timespans} ${update_rate} ${num_workers}

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
 ${num_edge_streaming}\
 ${timespans}\
 ${update_rate}\
 -w ${num_workers}\
 | tee -a latest.log
