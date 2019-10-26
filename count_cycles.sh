for i in {0..2}; do
  cnt=$(seq 21 30\
    | while read n; do
        if [ -f $1/worker${n}.log ]; then
          tail $1/worker${n}.log -n 100 | grep "${i} found" | awk '{print $4}'
        fi
      done\
    | awk '{s+=$1}END{print s}')
  echo "${i}: ${cnt}"
done

seq 21 30\
  | while read n; do
      if [ -f $1/worker${n}.log ]; then
        cat $1/worker${n}.log | grep latency | awk -F':' '{print $2}' | awk -F'ms' '{print $1}'
      fi
    done\
  | sort -n\
  > /tmp/latency

num=$(wc -l /tmp/latency | awk '{print $1}')
echo "50%: $(head /tmp/latency -n $(bc <<< ${num}*0.5/1) | tail -n 1)"
echo "90%: $(head /tmp/latency -n $(bc <<< ${num}*0.9/1) | tail -n 1)"
echo "99%: $(head /tmp/latency -n $(bc <<< ${num}*0.99/1) | tail -n 1)"

