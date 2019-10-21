scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
num_machines=10
mode=--release
#mode=
expr_time=300 # seconds

#graph=edge:${scriptdir}/toy_graph.edge
#num_edge_static=5
#timespans='2:6000,8000'
#update_rate=2
graph=edge:~/datasets/twitter.edge
num_edge_static=350000
timespans='2:172800000,86400000'
update_rate=3000
num_edge_streaming=$(bc -l <<< "${expr_time}*${update_rate}")
num_workers=10

function gen_hostfile {
  rm hostfile
  rm workers
  for i in 21 22 23 24 25 26 27 28 29 30; do
    port=$(shuf -i 20000-65000 -n 1)
    echo "172.16.105.${i}:${port}" >> hostfile
    echo "172.16.105.${i}" >> workers
  done
}

function local_run {
  log_file="$(hostname)_$1.log"
  touch ${scriptdir}/logs/${log_file}
  rm ${scriptdir}/logs/$(hostname).log
  cp -s ${scriptdir}/logs/${log_file} ${scriptdir}/logs/$(hostname).log

  RUST_BACKTRACE=full cargo run ${mode} --bin rtcd --\
    ${graph}\
    ${num_edge_static}\
    ${num_edge_streaming}\
    ${timespans}\
    ${update_rate}\
    -w ${num_workers}\
    -h ${scriptdir}/hostfile\
    -n ${num_machines}\
    -p $(bc -l <<< $(echo $(hostname) | sed s/worker//)-21)\
    2>&1 | tee -a ${scriptdir}/logs/${log_file}
}

if [ "$1" == "local" ]; then
  echo "Launching local timely process"
  local_run "$2"
else
  cargo build ${mode}
  gen_hostfile
  mkdir ${scriptdir}/logs > /dev/null

  echo "Calling a timely cluster"

  mkdir logs 2> /dev/null
  rm latest.log 2> /dev/null
  log_datetime=$(date | awk '{print $2$3"_"$4"_"$6}')
# touch ./logs/${log_datetime}.log
# cp -s ./logs/${log_datetime}.log latest.log

  pssh -t 0 -P -h ${scriptdir}/workers -x "-t -t" "
    echo \$(hostname) && ulimit -n 4096 && cd ${scriptdir} && ls -la > /dev/null &&
    ${scriptdir}/dist_run.sh local ${log_datetime}
  "
fi
