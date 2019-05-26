use std::net::SocketAddr;
use tokio_core::net::UdpSocket;
use tokio_core::reactor::Core;
use futures::{Future, Stream};
use futures::stream;
use std::net::Ipv4Addr;
use rtp_rs;
use crate::store;

fn for_each_rtp<F>(socket: UdpSocket, f: F) -> impl Future<Item=(), Error=()>
    where
        F: FnMut(Result<rtp_rs::RtpReader, rtp_rs::RtpHeaderError>, SocketAddr) + Sized
{
    let mut buf = Vec::new();
    buf.resize(9000, 0);
    stream::unfold((socket, buf, f), |(socket, buf, mut f)| {
        let fut = socket
            .recv_dgram(buf)
            .and_then(|(sock, buf, size, addr)| {
                f(rtp_rs::RtpReader::new(&buf[..size]), addr);
                Ok( ((), (sock, buf, f)) )
            });
        Some(fut)
    }).for_each(|_| { Ok(()) })
        .map_err(|_| () )
}

pub fn tokio_main() {
    let addr = "0.0.0.0:5000".parse::<SocketAddr>().unwrap();
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let socket = UdpSocket::bind(&addr, &handle).unwrap();
    /*
    let group = Ipv4Addr::new(239,100,0,1);
    let iface = Ipv4Addr::new(0,0,0,0);
    socket.join_multicast_v4(&group, &iface).expect("failed to join multicast group");
    */

    let store = store::Store::new();

    let mut expected_seq = None;
    let (mut ctx, mut demux) = crate::mpegts::create_demux(store.clone());
    let recv = for_each_rtp(socket, move |rtp, addr| {
        match rtp {
            Ok(rtp) => {
                let this_seq = rtp.sequence_number();
                if let Some(seq) = expected_seq {
                    if this_seq != seq {
                        println!(
                            "RTP: sequence mismatch: expected {:?}, got {:?}",
                            seq,
                            rtp.sequence_number()
                        );
                    }
                }
                expected_seq = Some(this_seq.next());
                //println!("got a packet from {:?}, seq {}", addr, rtp.sequence_number());
                demux.push(&mut ctx, rtp.payload());
            },
            Err(e) => {
                println!("rtp error from {:?}: {:?}", addr, e);
            }
        }
    });
    let http_server = crate::http::create_server(store);
    let future = recv.select(http_server);
    match core.run(future) {
        Ok(_) => (),
        Err(e) => panic!("Core::run() failed"),
    }
}
