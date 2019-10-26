scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function run_rtcd_once {
  update_rate=$1
  num_machines=$2
  num_workers=$3
  cur_dir=$(pwd)

  log_dir="U${update_rate}_N${num_machines}_W${num_workers}"
  rm ${log_dir} -r
  mkdir ${log_dir}

  echo "send_to_zzxx To run RTCD on Timely with @NL@"\
  "update_rate ${update_rate}@NL@num_machines ${num_machines}@NL@num_workers ${num_workers}"\
  > /data/zzxx/play/itchat/FIFO

  cp dist_run.sh dist_run.gen.sh
  sed -i "2c num_machines=${num_machines}" dist_run.gen.sh
  sed -i "17c update_rate=${update_rate}" dist_run.gen.sh
  sed -i "19c num_workers=${num_workers}" dist_run.gen.sh
  sed -i "49c > $(pwd)/${log_dir}/\$(hostname).log" dist_run.gen.sh
  sed -i "61c \${scriptdir}/dist_run.gen.sh local \${log_datetime}" dist_run.gen.sh

  sleep 5
  echo "Run now"
  ${scriptdir}/dist_run.gen.sh
  echo "Done"

  mv dist_run.gen.sh $(pwd)/${log_dir}

  echo "send_to_zzxx $(${scriptdir}/count_cycles.sh $(pwd)/${log_dir} | /data/zzxx/play/itchat/newlinex)" > /data/zzxx/play/itchat/FIFO
  echo "send_to_zzxx Done RTCD on Timely with @NL@"\
  "update_rate ${update_rate}@NL@num_machines ${num_machines}@NL@num_workers ${num_workers}"\
  > /data/zzxx/play/itchat/FIFO
}

#for n in 2000 4000 6000 8000 10000; do
#  for w in 5 10 15 20; do
#    run_rtcd_once ${n} 1 ${w}
#  done
#  for w in 5 10 15; do
#    run_rtcd_once ${n} 10 ${w}
#  done
#done

#run_rtcd_once 10000 1 15
#run_rtcd_once 10000 1 20
run_rtcd_once 10000 10 5
run_rtcd_once 10000 10 10

#run_rtcd_once 2000 1 5
#run_rtcd_once 2000 10 5
#run_rtcd_once 10000 10 20
