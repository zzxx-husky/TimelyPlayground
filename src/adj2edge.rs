extern crate rand;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

#[allow(dead_code)]
pub fn adj2edge(adj_file: &String) {
  println!("Convering {} to edge file", adj_file);

  let mut edge_data = Vec::new();
  match File::open(adj_file) {
    Ok(file) => {
      let mut reader = BufReader::new(file);
      let mut line = String::new();
      while let Ok(num_bytes) = reader.read_line(&mut line) {
        if num_bytes == 0 {
          break;
        }
        let mut iter = line.split_whitespace();
        let src = iter.next().unwrap().parse::<u32>().unwrap();
        let num = iter.next().unwrap().parse::<u32>().unwrap();
        for _i in 0..num {
          let dst = iter.next().unwrap().parse::<u32>().unwrap();
          edge_data.push((src, dst));
        }
        line.clear();
      }
    }
    Err(e) => panic!(e)
  }

  edge_data.shuffle(&mut thread_rng());

  let edge_file_name: String = format!("{}.edge", adj_file);
  match File::create(edge_file_name) {
    Ok(file) => {
      let mut writer = BufWriter::new(file);
      for e in edge_data {
        writer.write(format!("{} {}\n", e.0, e.1).as_bytes()).unwrap();
      }
    }
    Err(e) => panic!(e)
  }
}
