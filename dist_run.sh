scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
num_machines=10
mode=release
#mode=debug
if [ "${mode}" = "release" ]; then
  build_opt="--release"
fi
expr_time=300 # seconds

#graph=edge:${scriptdir}/toy_graph.edge
#num_edge_static=5
#timespans='2:6000,8000'
#update_rate=2
graph=edge:~/datasets/twitter.edge
num_edge_static=3500000
timespans='3:3#172800000,4#86400000,5#43200000'
update_rate=10000
num_edge_streaming=$(bc -l <<< "${expr_time}*${update_rate}")
num_workers=5

function gen_hostfile {
  rm hostfile
  rm workers
  for ((i=0;i<${num_machines};i+=1)) do
    m=$(bc -l <<< ${i}+21)
    port=$(shuf -i 20000-65000 -n 1)
    echo "172.16.105.${m}:${port}" >> hostfile
    echo "172.16.105.${m}" >> workers
  done
}

function local_run {
  log_file="$(hostname)_$1.log"
  touch ${scriptdir}/logs/${log_file}
  rm ${scriptdir}/logs/$(hostname).log
  cp -s ${scriptdir}/logs/${log_file} ${scriptdir}/logs/$(hostname).log

  ls -la ${scriptdir}/target/${mode} > /dev/null && 
  RUST_BACKTRACE=full ${scriptdir}/target/${mode}/rtcd \
    ${graph}\
    ${num_edge_static}\
    ${num_edge_streaming}\
    ${timespans}\
    ${update_rate}\
    -w ${num_workers}\
    -h ${scriptdir}/hostfile\
    -n ${num_machines}\
    -p $(bc -l <<< $(echo $(hostname) | sed s/worker//)-21) \
    > ${scriptdir}/logs/$(hostname).log 2>&1
}

function dist_run {
  echo "Calling a timely cluster"

  mkdir logs 2> /dev/null
  rm latest.log 2> /dev/null
  log_datetime=$(date | awk '{print $2$3"_"$4"_"$6}')

  pssh -t 0 -P -h ${scriptdir}/workers -x "-t -t" "
    echo \$(hostname) && ulimit -n 4096 && cd ${scriptdir} && ls -la > /dev/null &&
    ${scriptdir}/dist_run.sh local ${log_datetime}
  "
}

if [ "$1" == "local" ]; then
  echo "Launching local timely process"
  local_run "$2"
else
  mkdir ${scriptdir}/logs 2> /dev/null
  cargo build ${build_opt} && gen_hostfile && dist_run
fi
