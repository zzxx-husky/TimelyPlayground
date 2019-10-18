#cargo run --release --bin rtcd -- -w 1
mode="--release"
echo $1
if [ "$1" = "debug" ] || [ "$1" = "build" ]; then
  mode=
fi
RUST_BACKTRACE=1 cargo run ${mode} --bin rtcd --\
 toy_graph\
 5\
 2:6000,8000\
 2\
 -w 2\
 | tee -a logs
