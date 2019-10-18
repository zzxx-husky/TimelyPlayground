extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::*;
use self::timely::dataflow::channels::pact::Pipeline;

/**
 * Timely dataflow keeps tracks of the timestamps used (e.g., stored by the users) inside a operator
 * The boundary formed by these used timestamps is the frontier of the output of the operator
 * Therefore, the frontier is automatically updated by timely without user efforts.
 */

#[allow(dead_code)]
pub fn playing() {
  timely::execute_from_args(std::env::args(), |worker| {
    let mut input = InputHandle::new();
    let mut probe = ProbeHandle::new();

    worker.dataflow::<usize, _, _>(|outer| {
      let stream = input.to_stream(outer);
      stream
        .unary_frontier(
          Pipeline,
          "FrontierCheck",
          |_capability, _info| {
            let mut sometime = None;
            move |input, output| {
              let frontier = input.frontier();

              input.for_each(|time, recs| {
                let mut vec = Vec::new();
                recs.swap(&mut vec);
                println!("1st: {:?} {:?} {:?}", frontier, time, vec);
                assert_eq!(vec.len(), 1);
                let i = vec[0];
                if i != 200 {
                  output.session(&time).give(i);
                } else {
                  sometime = Some(time.retain())
                }
              });

              if !frontier.less_equal(&3) && frontier.less_than(&5) {
                match &sometime {
                  Some(t) => {
                    output.session(&t).give(200);
                  }
                  None => {
                    panic!("!!!");
                  }
                }
                sometime = None;
              }
            }
          },
        )
        .unary_frontier(
          Pipeline,
          "Inspect",
          |_capability, _info| {
            move |input, output| {
              let frontier = input.frontier();

              input.for_each(|time, recs| {
                let mut vec = Vec::new();
                recs.swap(&mut vec);
                println!("2nd: {:?} {:?} {:?}", frontier, time, vec);
                output.session(&time).give(1);
              });
            }
          },
        )
        .probe_with(&mut probe);
    });

    input.advance_to(1);
    worker.step();
    input.send(100);
    worker.step();
    input.advance_to(2);
    worker.step();
    input.send(200);
    worker.step();
    input.advance_to(3);
    worker.step();
    input.send(300);
    worker.step();
    input.advance_to(4);
    while probe.less_than(&input.time()) {
      worker.step();
    }
    input.advance_to(5);
    while probe.less_than(&input.time()) {
      worker.step();
    }
    input.send(400);
    input.advance_to(6);
    while probe.less_than(&input.time()) {
      worker.step();
    }
  }).unwrap();
}