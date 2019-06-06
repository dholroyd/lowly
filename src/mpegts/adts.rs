use mpeg2ts_reader::{packet, pes, psi, descriptor};
use crate::mpegts::IngestDemuxContext;
use crate::store;

struct IngestAdtsConsumer {
    store: store::Store,
    pid: packet::Pid,
    track_id: Option<store::TrackId>,
    last_pts: Option<pes::Timestamp>,
    last_dts: Option<pes::Timestamp>,
    max_bitrate: Option<u32>,
}
impl IngestAdtsConsumer {
    fn set_pts_dts(&mut self, pts: Option<pes::Timestamp>, dts: Option<pes::Timestamp>) {
        if let (Some(last_pts), Some(pts)) = (self.last_pts, pts) {
            if pts.likely_wrapped_since(last_pts) {
                eprint!("Oh no!  PTS wrap!");
            }
        }
        if let (Some(last_dts), Some(dts)) = (self.last_dts, dts) {
            if dts.likely_wrapped_since(last_dts) {
                eprint!("Oh no!  DTS wrap!");
            }
        }
        self.last_pts = pts;
        self.last_dts = dts;
    }
}
impl adts_reader::AdtsConsumer for IngestAdtsConsumer {
    fn new_config(&mut self, mpeg_version: adts_reader::MpegVersion, protection: adts_reader::ProtectionIndicator, aot: adts_reader::AudioObjectType, freq: adts_reader::SamplingFrequency, private_bit: u8, channels: adts_reader::ChannelConfiguration, originality: adts_reader::Originality, home: u8) {
        self.track_id = Some(self.store.allocate_aac_track(aot, freq, channels, self.max_bitrate));
        println!("ADTS {:?} new config: {:?} {:?} {:?} {:?} {:?} {:?} home={:?}", self.pid, mpeg_version, protection, aot, freq, channels, originality, home);
    }
    fn payload(&mut self, buffer_fullness: u16, no_of_blocks: u8, buf: &[u8]) {
        let pts = self.last_pts.map(|ts| ts.value() ).unwrap_or(0);
        self.store.add_aac_sample(self.track_id.unwrap(), store::Sample {
            header: store::SampleHeader::Aac,
            data: buf.to_vec(),
            pts,
            dts: self.last_dts.map(|ts| ts.value() ).unwrap_or(pts),
        })
    }
    fn error(&mut self, err: adts_reader::AdtsParseError) {
        println!("ADTS error: {:?}", err);
    }
}

pub struct AdtsElementaryStreamConsumer {
    parser: adts_reader::AdtsParser<IngestAdtsConsumer>,
}
impl AdtsElementaryStreamConsumer {
    pub fn construct(stream_info: &psi::pmt::StreamInfo, store: store::Store) -> pes::PesPacketFilter<IngestDemuxContext, AdtsElementaryStreamConsumer> {
        let mut max_bitrate = None;
        for desc in stream_info.descriptors::<descriptor::CoreDescriptors>() {
            match desc {
                Ok(d) => match d {
                    mpeg2ts_reader::descriptor::CoreDescriptors::MaximumBitrate(max) => {
                        // TODO: if we could already have allocated a store::AvcTrack by here,
                        //       we could pass the data in more directly, rather than bouncing it
                        //       via the IngestH264Context instance,
                        max_bitrate = Some(max.maximum_bits_per_second());
                    }
                    _ => println!("  ADTS {:?}: {:?}", stream_info.elementary_pid(), d),
                }
                Err(e) => println!("  ADTS {:?}: Error reading descriptor: {:?}", stream_info.elementary_pid(), e),
            }
        }
        pes::PesPacketFilter::new(
            AdtsElementaryStreamConsumer {
                parser: adts_reader::AdtsParser::new(IngestAdtsConsumer {
                    store,
                    pid: stream_info.elementary_pid(),
                    track_id: None,
                    last_pts: None,
                    last_dts: None,
                    max_bitrate,
                })
            }
        )

    }
}
impl pes::ElementaryStreamConsumer for AdtsElementaryStreamConsumer {
    fn start_stream(&mut self) { println!("ADTS start_steam()"); }
    fn begin_packet(&mut self, header: pes::PesHeader) {
        match header.contents() {
            pes::PesContents::Parsed(Some(parsed)) => {
                match parsed.pts_dts() {
                    Ok(pes::PtsDts::PtsOnly(Ok(pts))) => self.parser.consumer.set_pts_dts(Some(pts), None),
                    Ok(pes::PtsDts::Both{pts:Ok(pts), dts:Ok(dts)}) => self.parser.consumer.set_pts_dts(Some(pts), Some(dts)),
                    _ => self.parser.consumer.set_pts_dts(None, None),
                }
                self.parser.push(parsed.payload());
            },
            pes::PesContents::Parsed(None) => println!("ADTS: Parsed(None)"),
            pes::PesContents::Payload(payload) => {
                println!("ADTS {:?} payload", self.parser.consumer.pid);
                self.parser.start();
                self.parser.push(payload);
            },
        }
    }
    fn continue_packet(&mut self, data: &[u8]) {
        //println!("ADTS: continue_packet() {}", data.len());
        self.parser.push(data);
    }
    fn end_packet(&mut self) { }
    fn continuity_error(&mut self) { }
}
