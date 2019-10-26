use super::messages::*;

use std::collections::HashMap;

struct Vertex {
  level: u32,
  neighbors: Vec<u32>,
}

pub struct CyclePattern {
  // the creation time of the request, used for measuring per record latency
  pub creation_time: u64,
  // const
  worker_idx: usize,
  operator_idx: usize,
  subgraph_idx: usize,
  time_span: u64,
  max_cycle_length: u32,
  root: u32,

  // non-const
  // request_id: usize,
  graph: HashMap<u32, Vertex>,
  pub num_pending_replies: usize,
}

impl CyclePattern {
  pub fn create(creation_time: u64,
                worker_idx: usize,
                operator_idx: usize,
                subgraph_idx: usize,
                time_span: u64,
                max_cycle_length: u32) -> CyclePattern {
    CyclePattern {
      creation_time: creation_time,
      worker_idx: worker_idx,
      operator_idx: operator_idx,
      subgraph_idx: subgraph_idx,
      time_span: time_span,
      max_cycle_length: max_cycle_length,
      root: 0,
      graph: HashMap::new(),
      num_pending_replies: 0,
    }
  }

  pub fn add_starting_edge(&mut self, u: &UpdateRequest) -> Vec<FetchRequest> {
    self.graph.insert(u.src, Vertex { level: 0, neighbors: vec!(u.dst) });
    self.graph.insert(u.dst, Vertex { level: 1, neighbors: Vec::new() });
    self.root = u.src;
    let requests = vec!(FetchRequest {
      vertex_id: u.dst,
      worker_idx: self.worker_idx,
      operator_idx: self.operator_idx,
      subgraph_idx: self.subgraph_idx,
//      record_idx: self.request_id,
      time_span: self.time_span,
    });
//    self.request_id += 1;
    self.num_pending_replies += requests.len();
    requests
  }

  pub fn on_reply(&mut self, r: &FetchReply) -> Vec<FetchRequest> {
    self.num_pending_replies -= 1;
    let mut new_requests = Vec::new();
    let mut new_vertices = Vec::new();
    let mut lvldown_vtx: Vec<u32> = Vec::new();
    {
      let mut v = self.graph.get_mut(&r.vertex_id)
        .expect("Failed to find the vertex that makes the request");
      v.neighbors = r.neighbors.clone();
      let v_level = v.level; // not to ref to v ...
      if v_level < self.max_cycle_length {
        for n in r.neighbors.iter() {
          match self.graph.get_mut(&n) {
          // if !self.graph.contains_key(n) {
            Some(x) => {
              if x.level > v_level + 1 {
                x.level = v_level + 1;
                lvldown_vtx.push(*n);
              }
            },
            None => {
              new_vertices.push((*n, Vertex { level: v_level + 1, neighbors: Vec::new() }));
              new_requests.push(FetchRequest {
                vertex_id: *n,
                worker_idx: self.worker_idx,
                operator_idx: self.operator_idx,
                subgraph_idx: self.subgraph_idx,
                time_span: self.time_span,
              });
            }
          }
        }
      }
    }
    // Note that some early requests may arrive late, where a neighbor of a vertex with level x may be assigned a level larger than x+1.
    // The code below is try to fix the problem.
    // {
    //   while let Some(xi) = lvldown_vtx.pop() {
    //     let x = self.graph.get_mut(&xi).expect("There must be a BBBUUUGGG");
    //     let x_level = x.level;
    //     let vec = x.neighbors.clone(); // rust makes me clone, sorry
    //     for ni in vec.iter() {
    //       match self.graph.get_mut(ni) {
    //         Some(n) => {
    //           if n.level > x_level + 1 {
    //             if n.level == self.max_cycle_length {
    //               new_requests.push(FetchRequest {
    //                 vertex_id: *ni,
    //                 worker_idx: self.worker_idx,
    //                 operator_idx: self.operator_idx,
    //                 subgraph_idx: self.subgraph_idx,
    //                 time_span: self.time_span,
    //               });
    //             }
    //             n.level = x_level + 1;
    //           }
    //         },
    //         None => {
    //           new_vertices.push((*ni, Vertex { level: x_level + 1, neighbors: Vec::new() }));
    //           new_requests.push(FetchRequest {
    //             vertex_id: *ni,
    //             worker_idx: self.worker_idx,
    //             operator_idx: self.operator_idx,
    //             subgraph_idx: self.subgraph_idx,
    //             time_span: self.time_span,
    //           });
    //         }
    //       }
    //     }
    //   }
    // }
    self.graph.extend(new_vertices);
    self.num_pending_replies += new_requests.len();
    new_requests
  }

  fn recursive_detect(&self, root_level: usize, imme_path: &mut Vec<u32>) -> u32 {
    let mut num_results = 0;
    // if root_level < 5 {
    if root_level < self.max_cycle_length as usize - 1 {
      match self.graph.get(&imme_path[root_level]) {
        Some(node) => {
          for n in node.neighbors.iter() {
            imme_path.push(*n);
            num_results += self.recursive_detect(root_level + 1, imme_path);
            imme_path.pop();
          }
        }
        None => {} // it's fine
      }
    } else {
      match self.graph.get(&imme_path[root_level]) {
        Some(node) => {
          if node.neighbors.contains(&self.root) {
            imme_path.push(self.root);
            // println!("Subgraph {}, Operator {}, Worker {}, Found: {:?}", self.subgraph_idx, self.operator_idx, self.worker_idx, imme_path);
            // println!("Found: {:?}", imme_path);
            num_results += 1;
            imme_path.pop();
          }
        }
        None => {} // it's fine
      }
    }
    num_results
  }

  pub fn detect_cycles(&self) -> u32 {
    let mut imme_path = vec!(self.root);
    let num_results = self.recursive_detect(0, &mut imme_path);
    // println!("Found {} using {}ms", num_results, SystemTime::now().duration_since(self.creation_time).unwrap().as_millis());
    num_results
  }
}

