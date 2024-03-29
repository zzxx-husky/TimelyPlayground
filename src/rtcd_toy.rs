extern crate timely;
extern crate priority_queue;

use crate::utils::subgraph::*;
use crate::utils::messages::*;

use priority_queue::PriorityQueue;
use std::cmp::min;
use std::collections::{HashMap, BTreeMap};
use std::ops::Bound::Included;
use std::time::{SystemTime, UNIX_EPOCH};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::scopes::Scope;
use timely::order::Product;

#[allow(dead_code)]
pub fn rtcd() {
  timely::execute_from_args(std::env::args(), |worker| {
    let mut input = InputHandle::new();
    let mut probe = ProbeHandle::new();
    let worker_idx = worker.index();

    worker.dataflow::<u64, _, _>(|outer| {
      let outer_edge_updates = input.to_stream(outer);
      outer.iterative(|inner| {
        // push outer data into inner cycle
        let edge_updates = outer_edge_updates.enter(inner);

        let (handle, fetch_replies) = inner.feedback(Product::new(0, 1)); // increase iteration
        fetch_replies.leave().probe_with(&mut probe);

        let mut detections = Vec::new();
        // both detects cycle of length 6 but with different time span
        for time_span_value in vec!(10, 20) {
          let operator_idx_value = detections.len() + 1;
          let reply_router = fetch_replies.filter(move |r: &FetchReply| r.operator_idx == operator_idx_value);

          detections.push(edge_updates
            .binary_frontier(
              &reply_router,
              Exchange::new(|u: &UpdateRequest| u.src as u64),
              Exchange::new(|r: &FetchReply| r.worker_idx as u64),
              &("DetectionOnSpan:".to_string() + &time_span_value.to_string()),
              |_capability, _info| {
                let time_span = time_span_value;
                let operator_idx = operator_idx_value;

                let mut subgraphs: HashMap<usize, CyclePattern> = HashMap::new();
                let mut subgraph_idx = 0;

                move |updates, replies, output| {
                  { // process replies first
                    let mut vector: Vec<FetchReply> = Vec::new();
                    replies.for_each(|time, reqs| {
                      reqs.swap(&mut vector);
                      for r in vector.drain(..) {
//                        println!("{:?} {:?}", time, r);
                        match subgraphs.get_mut(&r.subgraph_idx) {
                          Some(graph) => {
                            let mut requests = graph.on_reply(&r);
                            if requests.is_empty() {
                              if graph.num_pending_replies == 0 {
                                graph.detect_cycles();
                                subgraphs.remove(&r.subgraph_idx);
                              }
                            } else {
                              output.session(&time).give_vec(&mut requests);
                            }
                          }
                          None => panic!("No local record makes this request!")
                        }
                      }
                    });
                  }
                  {
                    let mut vector: Vec<UpdateRequest> = Vec::new();
                    updates.for_each(|time, reqs| {
                      reqs.swap(&mut vector);
                      for u in vector.drain(..) {
//                        println!("{:?} {:?}", time.time(), u);
                        let mut graph = CyclePattern::create(u.creation_time, worker_idx, operator_idx, subgraph_idx, time_span, 6);
                        let mut requests = graph.add_starting_edge(&u);
                        subgraphs.insert(subgraph_idx, graph);
                        subgraph_idx += 1;
                        output.session(&time).give_vec(&mut requests);
                      }
                    });
                  }
                }
              }));
        }

        let fetch_requests = detections.get(0).unwrap()
          .binary(
            &detections.get(1).unwrap(),
            Pipeline,
            Pipeline,
            "FetchRequestMerger",
            |_capability, _info| {
              let mut vector = Vec::new();
              move |reqs1, reqs2, output| {
                reqs1.for_each(|time, reqs| {
                  reqs.swap(&mut vector);
                  output.session(&time).give_vec(&mut vector);
                });
                reqs2.for_each(|time, reqs| {
                  reqs.swap(&mut vector);
                  output.session(&time).give_vec(&mut vector);
                });
              }
            },
          );

        let _changes = edge_updates
          .binary_frontier(
            &fetch_requests,
            Exchange::new(|u: &UpdateRequest| u.src as u64),
            Exchange::new(|f: &FetchRequest| f.vertex_id as u64),
            "StateAccess",
            |_capability, _info| {
              let mut adj_lists = HashMap::new();
              let mut pending_requests = PriorityQueue::new();

              move |updates, fetch_requests, output| {
                {
                  let mut vector = Vec::new();
                  updates.for_each(|time, reqs| {
                    reqs.swap(&mut vector);
                    for u in vector.drain(..) {
//                    println!("{:?} {:?}", time.time(), u);
                      adj_lists.entry(u.src).or_insert(BTreeMap::new()) // find the vertex
                        .entry(time.time().outer).or_insert(Vec::new()) // find the time
                        .push(u.dst); // add the new neighbor
                    }
                  });
                }

//                println!("{:?} {:?}", updates.frontier(), fetch_requests.frontier());
                let frontier = updates.frontier(); // the update progress
                {
                  let mut vector = Vec::new();
                  fetch_requests.for_each(|time, reqs| {
                    reqs.swap(&mut vector);
                    let ts = *time.time();
                    let t_outer = ts.outer;
                    let t_cap = time.retain();
                    let mut reqs = Vec::new();
                    for r in vector.drain(..) {
//                    println!("{:?}", r);
                      if frontier.less_than(&ts) {
                        let neighbors: Vec<_> = adj_lists.entry(r.vertex_id).or_insert(BTreeMap::new()) // find the vertex
                          .range((Included(t_outer - min(t_outer, r.time_span)), Included(t_outer)))
                          .flat_map(|e| e.1)
                          .map(|i| *i)
                          .collect();
//                      println!("Reply {:?}", r);
                        output.session(&t_cap).give(r.reply(&neighbors));
                      } else {
//                      println!("Pending {:?}", r);
                        reqs.push(r);
                      }
                    }
                    pending_requests.push((t_cap, reqs), FetchRequestTimestamp { timestamp: ts });
                  });
                }

//              println!("{:?}", frontier);
//              if !pending_requests.is_empty() {
//                println!("{:?}", pending_requests.peek().unwrap().1);
//                println!("{:?}", pending_requests);
//              }
                while let Some(req) = pending_requests.peek() {
                  let ts = req.1.timestamp;
                  let t_outer = ts.outer;
                  if !frontier.less_equal(&ts) { // t < U ==> U > t ==> !(U <=t)
                    let cap = &(req.0).0;
                    for r in &(req.0).1 {
                      let neighbors: Vec<_> = adj_lists.entry(r.vertex_id).or_insert(BTreeMap::new()) // find the vertex
                        .range((Included(t_outer - min(t_outer, r.time_span)), Included(t_outer)))
                        .flat_map(|e| e.1)
                        .map(|i| *i)
                        .collect();
//                    if r.subgraph_idx == 0 {
//                      println!("{:?} {:?} {:?}", (Included(time - min(time, r.time_span)), Included(time)), neighbors, adj_lists);
//                    }
//                    println!("Reply pending {:?}", r);
                      output.session(&cap).give(r.reply(&neighbors));
                    }
                    pending_requests.pop();
                  } else {
                    break;
                  }
                }
              }
            })
          .connect_loop(handle);
      });
    });

    let expr_start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    for i in 0u32..10 {
      input.advance_to(i as u64);
      if i as usize % worker.peers() == worker_idx {
        input.send(UpdateRequest {
          creation_time: expr_start_time,
          src: i,
          dst: i + 1,
          is_basic: false,
        });
      }
      if i >= 5 {
        if i as usize % worker.peers() == worker_idx {
          input.send(UpdateRequest {
            creation_time: expr_start_time,
            src: i,
            dst: i - 5,
            is_basic: false,
          });
        }
      }
      worker.step();
    }
    while probe.less_than(input.time()) {
      worker.step();
    }
  }).unwrap();
}
