#[macro_use] 
// extern crate abomonation;
extern crate abomonation_derive;

mod abom;
mod adj2edge;
mod playing;
mod rtcd;
mod rtcd_toy;
mod utils;

fn main() {
//	abom::abom();
//  adj2edge::adj2edge(&(std::env::args().collect::<Vec<String>>())[1]);
  rtcd::rtcd();
//  rtcd_toy::rtcd();
//  playing::playing();
}
