extern crate abomonation;

use abomonation::Abomonation;
use std::cmp::Ordering;
use std::hash::{Hasher, Hash};
use timely::order::Product;
use std::time::SystemTime;

#[derive(Debug)]
pub struct FetchRequestTimestamp {
  pub timestamp: Product<u64, u32>,
}

impl Ord for FetchRequestTimestamp {
  // make the comparison reverse !!!
  fn cmp(&self, other: &FetchRequestTimestamp) -> Ordering {
    other.timestamp.cmp(&self.timestamp)
  }
}

impl PartialOrd for FetchRequestTimestamp {
  fn partial_cmp(&self, other: &FetchRequestTimestamp) -> Option<Ordering> {
    other.timestamp.partial_cmp(&self.timestamp)
  }
}

impl PartialEq for FetchRequestTimestamp {
  fn eq(&self, other: &FetchRequestTimestamp) -> bool {
    self.timestamp == other.timestamp
  }
}

impl Eq for FetchRequestTimestamp {}

#[derive(Clone, Debug)]
pub struct UpdateRequest {
  pub creation_time: SystemTime, // the creation time of the request, used for measuring per record latency
  pub src: u32,
  pub dst: u32,
  pub is_basic: bool, // is for basic graph or streaming graph
//  pub is_addition: u32, // +1 for addition, -1 for deletion
}

#[derive(Clone, Debug)]
pub struct FetchRequest {
  pub vertex_id: u32,
  // one worker may detect multiple patterns, each has a different operator idx
  pub worker_idx: usize,
  // a pattern is being detected on multiple records
  pub operator_idx: usize,
  pub subgraph_idx: usize,
  //  pub record_idx: usize,
  pub time_span: u64,
}


#[derive(Clone, Debug)]
pub struct FetchReply {
  pub worker_idx: usize,
  pub operator_idx: usize,
  pub subgraph_idx: usize,
  //  pub record_idx: usize,
  pub vertex_id: u32,
  pub neighbors: Vec<u32>,
}

impl Abomonation for UpdateRequest {}

impl FetchRequest {
  pub fn reply(&self, neighbors: Vec<u32>) -> FetchReply {
    FetchReply {
      worker_idx: self.worker_idx,
      operator_idx: self.operator_idx,
      subgraph_idx: self.subgraph_idx,
//      record_idx: self.record_idx,
      vertex_id: self.vertex_id,
      neighbors: neighbors,
    }
  }
}

impl Abomonation for FetchRequest {}

impl Hash for FetchRequest {
  fn hash<H>(&self, state: &mut H) where H: Hasher {
    state.write_usize(self.worker_idx);
    state.write_usize(self.operator_idx);
    state.write_usize(self.subgraph_idx);
//    state.write_usize(self.record_idx);
    state.finish();
  }
}

impl PartialEq for FetchRequest {
  fn eq(&self, other: &FetchRequest) -> bool {
    self.worker_idx == other.worker_idx && self.operator_idx == other.operator_idx && self.subgraph_idx == other.subgraph_idx
  }
}

impl Eq for FetchRequest {}

impl Abomonation for FetchReply {}
