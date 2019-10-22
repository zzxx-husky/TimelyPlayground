use abomonation::{encode, decode};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

#[derive(Abomonation, Debug)]
struct S {
  val: usize,
  n: Vec<u32>
}

#[allow(dead_code)]
pub fn abom() {
  let args: Vec<_> = std::env::args().collect();
  let role = &args[1];
  let vector: Vec<_> = (0..256u32).map(|i| S{val: i as usize, n: vec!(i)}).collect();

  if role == "server" {
    println!("{:?}", vector);
    // encode vector into a Vec<u8>
    let mut bytes = Vec::new();
    unsafe { encode(&vector, &mut bytes).unwrap(); }
    println!("bytes: {}", bytes.len());

    let listener = TcpListener::bind("0.0.0.0:9123").unwrap();
    println!("Server starts");
    for stream in listener.incoming() {
      println!("Incoming transmission");
      let mut s = stream.unwrap();
      s.write(&bytes[..]).unwrap();
      println!("Sent");
    }
  }

  if role == "client" {
    match TcpStream::connect("127.0.0.1:9123") {
      Ok(mut stream) => {
        let mut data: Vec<u8> = Vec::with_capacity(10000);
        data.resize(10000, 0);
        println!("Connected");
        match stream.read(&mut data[..]) {
          Ok(size) => { // if len is 0, size will be 0, even the capacity is 100000 ...
            assert!(size < 10000);
            println!("{}", size);
            data.resize(size, 0);
            // unsafely decode a &Vec<(u64, String)> from binary data (maybe your utf8 are lies!).
            if let Some((result, remaining)) = unsafe { decode::<Vec<S>>(&mut data) } {
              assert!(result.len() == vector.len());
              assert!(remaining.len() == 0);
              println!("{:?}", result);
            }
          },
          Err(e) => panic!(e)
        }
      },
      Err(e) => panic!(e)
    };
  }
}
