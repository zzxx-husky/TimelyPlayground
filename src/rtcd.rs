extern crate timely;
extern crate priority_queue;

use crate::utils::subgraph::*;
use crate::utils::messages::*;

use priority_queue::PriorityQueue;
use std::cmp::min;
use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Bound::Included;
use std::time::{SystemTime, Instant, Duration, UNIX_EPOCH};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::scopes::Scope;
use timely::order::Product;

pub fn rtcd() {
  const MAX_DEGREE: usize = 1000;
  const MAX_QUERIES: usize = 3;
  static mut NUM_DETECTIONS: [[u32; MAX_DEGREE]; MAX_QUERIES] = [[0; MAX_DEGREE]; MAX_QUERIES];
  let mut edge_data = Vec::new();
  let num_edges_basic: usize;
  let num_edges_streaming: usize;
  let num_patterns: usize;
  let pattern_timespans: Vec<u64>;
  let cycle_lengths: Vec<u32>;
  let max_pattern_timespan: u64;
  let update_rate: usize;
  let num_configs: usize = 5;
  let mut args: Vec<String> = std::env::args().collect();
  let expr_start_time = SystemTime::now();
  {
    assert!(args.len() > num_configs, "\n\n\
Please provide the following configs in order:\n\
  1. graph format and graph data file, in the format 'adj:name' or 'edge:name'\n\
  2. the number of edges in the basic graph\n\
  3. the number of edges for the streaming graph\n\
  4. the time spans in millis of patterns in the format 'n:span1,span2,span3,span_n'\n\
  5. update rate, i.e., num of overall edge updates per second\n\
\n");
    {
      num_edges_basic = args[2].parse::<usize>().expect(&*format!("Invalid number of edges for static graph: {}", args[2]));
      println!("Number of edges for static graph: {}", num_edges_basic);
      num_edges_streaming = args[3].parse::<usize>().expect(&*format!("Invalid number of edges for streaming graph: {}", args[3]));
      println!("Number of edges for streaming graph: {}", num_edges_streaming);
    }
    {
      let fmt_filename: Vec<_> = args[1].split(':').collect();
      assert!(fmt_filename.len() == 2, "Invalid graph data file: ".to_owned() + &args[1]);
      let format = &fmt_filename[0];
      let edge_data_file = &fmt_filename[1];
      println!("Loading graph data {} into memory", edge_data_file);
      let timer = Instant::now();
      match File::open(edge_data_file) {
        Ok(file) => {
          let mut reader = BufReader::new(file);
          let mut line = String::new();
          match format {
            &"edge" => {
              let num_edges = num_edges_basic + num_edges_streaming;
              while let Ok(num_bytes) = reader.read_line(&mut line) {
                if num_bytes == 0 || edge_data.len() > num_edges {
                  break;
                }
                let mut iter = line.split_whitespace();
                let src = iter.next().unwrap().parse::<u32>().unwrap();
                let dst = iter.next().unwrap().parse::<u32>().unwrap();
                edge_data.push((src, dst));
//            println!("{:?}", (src, dst));
//            std::thread::sleep(Duration::new(1, 0));
                line.clear();
              }
            }
            &"adj" => {
              panic!("Adj format is not recommended because it makes the edge updates to dense. Convert Adj to Edge instead.");
//              while let Ok(num_bytes) = reader.read_line(&mut line) {
//                if num_bytes == 0 {
//                  break;
//                }
//                let mut iter = line.split_whitespace();
//                let src = iter.next().unwrap().parse::<u32>().unwrap();
//                let num = iter.next().unwrap().parse::<u32>().unwrap();
//                for _i in 0..num {
//                  let dst = iter.next().unwrap().parse::<u32>().unwrap();
//                  edge_data.push((src, dst));
//                }
//                line.clear();
//              }
            }
            _ => panic!("Known format: ".to_owned() + format)
          }
        }
        Err(e) => panic!(e)
      }
      println!("Loaded graph data {} within {:?}. Totally {} edges.", edge_data_file, timer.elapsed(), edge_data.len());
    }
    {
      let n_timespans: Vec<_> = args[4].split(':').collect();
      num_patterns = n_timespans[0].parse::<usize>().expect(&*format!("Invalid timespans: {}", args[4]));
      let len_span: Vec<_> = n_timespans[1].split(',')
        .map(|s| s.split('#').collect())
        .map(|s: Vec<_>| (s[0].parse::<u32>().expect(&*format!("Invalid len and span: {:?}", s)),
                          s[1].parse::<u64>().expect(&*format!("Invalid len and span: {:?}", s))))
        .collect();
      cycle_lengths = len_span.iter().map(|s| s.0).collect();
      pattern_timespans = len_span.iter().map(|s| s.1).collect();
      assert!(num_patterns == pattern_timespans.len());
      max_pattern_timespan = *pattern_timespans.iter().max_by(|a, b| a.cmp(b)).unwrap();
      println!("Timespans for cycles: {:?}. Max: {} ms.", pattern_timespans, max_pattern_timespan);
    }
    {
      update_rate = args[5].parse::<usize>().expect(&*format!("Invalid update rate: {}", args[5]));
      println!("Update rate: {} updates per second", update_rate);
    }
  }

  args.swap(0, num_configs);
  timely::execute_from_args(args.into_iter(), move |worker| {
    let mut input = InputHandle::new();
    let mut probe = ProbeHandle::new();
    let worker_idx = worker.index();
    let num_workers = worker.peers();
    assert!(worker_idx < MAX_DEGREE, "Hey, you should make MAX_DEGREE larger");
    assert!(pattern_timespans.len() <= MAX_QUERIES, "Hey, you should make MAX_QUERIES larger");
    println!("I am worker {} among {} workers.", worker_idx, num_workers);

    worker.dataflow::<u64, _, _>(|outer| {
      let outer_edge_updates = input.to_stream(outer);
      outer.iterative(|inner| {
        // push outer data into inner cycle
        let edge_updates = outer_edge_updates.enter(inner);

        let (handle, fetch_replies) = inner.feedback(Product::new(0, 1)); // increase iteration

        let mut detections = Vec::new();
        // both detects cycle of length 6 but with different time span
        for qid in 0..pattern_timespans.len() {
          let time_span_value = pattern_timespans[qid];
          let cycle_length = cycle_lengths[qid];
          let operator_idx_value = detections.len() + 1;
          let reply_router = fetch_replies.filter(move |r: &FetchReply| r.operator_idx == operator_idx_value);

          detections.push(edge_updates
            .filter(|u: &UpdateRequest| !u.is_basic)
            .binary_frontier(
              &reply_router,
              //Exchange::new(|u: &UpdateRequest| u.src as u64),
              Pipeline,
              Exchange::new(|r: &FetchReply| r.worker_idx as u64),
              &("DetectionOnSpan:".to_string() + &time_span_value.to_string()),
              |_capability, _info| {
                let time_span = time_span_value;
                let operator_idx = operator_idx_value;

                let mut subgraphs: HashMap<usize, CyclePattern> = HashMap::new();
                let mut subgraph_idx = 0;

                let mut sum_latencies = 0u64;
                let mut cnt_latencies = 0;
                let report_period = update_rate / num_workers + 1;

                move |updates, replies, output| {
                  { // process replies first
                    let mut vector = Vec::new();
                    replies.for_each(|time, reqs| {
                      reqs.swap(&mut vector);
                      for r in vector.drain(..) {
                        match subgraphs.get_mut(&r.subgraph_idx) {
                          Some(graph) => {
                            let mut requests = graph.on_reply(&r);
                            if requests.is_empty() {
                              if graph.num_pending_replies == 0 {
                                let num = graph.detect_cycles();
                                unsafe {
                                  NUM_DETECTIONS[qid][worker_idx] += num;
                                }
                                let cur_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                                sum_latencies += cur_time - graph.creation_time;
                                cnt_latencies += 1;
                                if cnt_latencies == report_period {
                                  println!("{}, latency: {}ms", qid, sum_latencies as f64 / cnt_latencies as f64);
                                  sum_latencies = 0;
                                  cnt_latencies = 0;
                                }
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
                    let mut vector = Vec::new();
                    updates.for_each(|time, reqs| {
                      reqs.swap(&mut vector);
                      for u in vector.drain(..) {
                        let mut graph = CyclePattern::create(u.creation_time,
                                                             worker_idx,
                                                             operator_idx,
                                                             subgraph_idx,
                                                             time_span, cycle_length);
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

        let mut fetch_requests = &detections[0];
        let mut merged_fetch_requests;
        for i in 1..detections.len() {
          merged_fetch_requests = fetch_requests
            .binary(
              &detections[i],
              Pipeline,
              Pipeline,
              "FetchRequestMerger",
              |_capability, _info| {
                move |reqs1, reqs2, output| {
                  reqs1.for_each(|time, reqs| {
                    let mut vector = Vec::new();
                    reqs.swap(&mut vector);
                    output.session(&time).give_vec(&mut vector);
                  });
                  reqs2.for_each(|time, reqs| {
                    let mut vector = Vec::new();
                    reqs.swap(&mut vector);
                    output.session(&time).give_vec(&mut vector);
                  });
                }
              },
            );
          fetch_requests = &merged_fetch_requests;
        }

        let state = edge_updates
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
                      // println!("{:?} {:?}", *time.time(), u);
                      adj_lists.entry(u.src).or_insert(BTreeMap::new()) // find the vertex
                        .entry(time.time().outer).or_insert(Vec::new()) // find the time
                        .push(u.dst); // add the new neighbor
                        // println!("{} {:?}", u.src, adj_lists[&u.src]);
                    }
                  });
                }

                let frontier = updates.frontier(); // the update progress
                // println!("{:?}", frontier);
                {
                  let mut vector = Vec::new();
                  fetch_requests.for_each(|time, reqs| {
                    reqs.swap(&mut vector);
                    let ts = *time.time();
                    let t_outer = ts.outer;
                    let t_cap = time.retain();
                    let mut reqs = Vec::new();
                    for r in vector.drain(..) {
                      if !frontier.less_equal(&ts) {
                        let neighbors: Vec<_> = adj_lists.entry(r.vertex_id).or_insert(BTreeMap::new()) // find the vertex
                          .range((Included(t_outer - r.time_span), Included(t_outer)))
                          .flat_map(|e| e.1)
                          .map(|i| *i)
                          .collect();
                        output.session(&t_cap).give(r.reply(&neighbors));
                      } else {
                        reqs.push(r);
                      }
                    }
                    pending_requests.push((t_cap, reqs), FetchRequestTimestamp { timestamp: ts });
                  });
                }

                while let Some(req) = pending_requests.peek() {
                  let ts = req.1.timestamp;
                  let t_outer = ts.outer;
                  if !frontier.less_equal(&ts) { // t < U ==> U > t ==> !(U <=t)
                    let cap = &(req.0).0;
                    for r in &(req.0).1 {
                      let neighbors: Vec<_> = adj_lists.entry(r.vertex_id).or_insert(BTreeMap::new()) // find the vertex
                        .range((Included(t_outer - r.time_span), Included(t_outer)))
                        .flat_map(|e| e.1)
                        .map(|i| *i)
                        .collect();
                      // println!("{} {} {:?}->{:?}", t_outer-r.time_span, t_outer, r, r.reply(&neighbors));
                      // println!("{:?}", adj_lists[&r.vertex_id]);
                      output.session(&cap).give(r.reply(&neighbors));
                    }
                    pending_requests.pop();
                  } else {
                    break;
                  }
                }
              }
            });

        state.leave().probe_with(&mut probe);
        state.connect_loop(handle);
      });
    });

    // Send basic graph data
    // We assign timestamp to records all based on expr_start_millis so as for consistency
    println!("Worker {} starts pushing static graph data.", worker_idx);
    let expr_start_millis = max_pattern_timespan as u64; // expr_start_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let mut static_prog = 10;
    let mut old_advance = 0u64;
    let mut basic_data = Vec::new();
    for i in 0..num_edges_basic {
      if i % num_workers == worker_idx {
        let rollback = (num_edges_basic - i) as f64 / num_edges_basic as f64 * (max_pattern_timespan as f64);
        let new_advance = expr_start_millis - rollback as u64;
        if old_advance < new_advance {
          if 0 != basic_data.len() {
            input.send_batch(&mut basic_data);
          }
          input.advance_to(new_advance);
          while probe.less_than(input.time()) {
            worker.step();
          }
          basic_data.clear();
          old_advance = new_advance;
        }
        basic_data.push(UpdateRequest {
          creation_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
          src: edge_data[i].0,
          dst: edge_data[i].1,
          is_basic: true,
        });
      }
      if worker_idx == 0 && i * 100 / num_edges_basic == static_prog {
        println!("{}({}{}) edges loaded.", i, static_prog, '%');
        static_prog += 10;
      }
    }
    assert!(old_advance <= expr_start_millis, "Invalid watermark for basic graph");
    if old_advance < expr_start_millis {
      if 0 != basic_data.len() {
        input.send_batch(&mut basic_data);
      }
      input.advance_to(expr_start_millis);
      old_advance = expr_start_millis;
    }
    while probe.less_than(input.time()) {
      worker.step();
    }
    println!("Worker {} takes {:?} for pushing basic graph data.", worker_idx, SystemTime::now().duration_since(expr_start_time).unwrap());
    // Send streaming graph data
    println!("Worker {} starts pushing streaming graph data.", worker_idx);
    for i in (num_edges_basic..edge_data.len()).step_by(update_rate) {
      let timer = Instant::now();
      let end_idx = min(edge_data.len(), i + update_rate);
      let mut updates: Vec<_> = (i..end_idx)
        .filter(|j| j % num_workers == worker_idx)
        .map(|j| UpdateRequest {
          creation_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
          src: edge_data[j].0,
          dst: edge_data[j].1,
          is_basic: false,
        })
        .collect();
      input.send_batch(&mut updates);
      let new_advance = old_advance + 1000; // ((end_idx - num_edges_basic) as f64 / update_rate as f64 * 1000f64) as u64 + expr_start_millis;
      if old_advance < new_advance {
        input.advance_to(new_advance);
        old_advance = new_advance;
      }
      println!("Worker {} pushed edge updates.", worker_idx);
      while probe.less_than(input.time()) {
        worker.step();
      }
      println!("Worker {} done for this batch.", worker_idx);
      let now_dur = timer.elapsed().as_millis() as u64;
      if 999 > now_dur { // give 1 ms to run the sleeping code
        std::thread::sleep(Duration::from_millis(999 - now_dur));
      }
    }
    println!("Worker {} has done all the jobs!", worker_idx);
  }).unwrap();
  unsafe {
    for i in 0..MAX_QUERIES {
      let sum: u32 = NUM_DETECTIONS[i].iter().sum();
      if sum > 0 {
        println!("Query {} found {}", i, sum);
      }
    }
  }
  println!("Now terminate");  
}
