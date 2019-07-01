use mpeg2ts_reader::{pes, packet, psi, descriptor};
use std::cell::RefCell;
use crate::mpegts::IngestDemuxContext;
use crate::store;
use h264_reader::{nal, rbsp};
use h264_reader::nal::{NalHeader, NalHandler};
use h264_reader::rbsp::RbspDecoder;
use std::collections::HashMap;
use h264_reader::nal::pps::ParamSetId;
use std::time::{SystemTime, SystemTimeError, Duration};
use mpeg2ts_reader::pes::Timestamp;

enum SliceType {
    Idr,
    NonIdr,
}

struct InProgressSlice {
    header: h264_reader::nal::NalHeader,
    buf: Vec<u8>,
}

struct SliceIngest {
    slice_type: SliceType,
    current_slice: Option<InProgressSlice>,
}
impl SliceIngest {
    pub fn new(slice_type: SliceType) -> SliceIngest {
        SliceIngest {
            slice_type,
            current_slice: None,
        }
    }
}
impl h264_reader::nal::NalHandler for SliceIngest {
    type Ctx = IngestH264Context;

    fn start(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, header: h264_reader::nal::NalHeader) {
        let mut buf = Vec::new();
        buf.push(header.into());
        self.current_slice = Some(InProgressSlice {
            header,
            buf,
        });
    }

    fn push(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, buf: &[u8]) {
        self.current_slice
            .as_mut()
            .unwrap()
            .buf
            .extend_from_slice(buf);
    }

    fn end(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>) {
        let current_slice = self.current_slice.take().unwrap();
        let capture = NalCapture::default();
        let mut decode = RbspDecoder::new(capture);
        decode.start(ctx, current_slice.header);
        decode.push(ctx, &current_slice.buf[..]);
        decode.end(ctx);
        let capture = decode.into_handler();
        let mut r = rbsp::RbspBitReader::new(&capture.buf[1..]);
        match nal::slice::SliceHeader::read(ctx, &mut r, current_slice.header) {
            Ok((header, sps, pps)) => {
                //println!("{:#?}", header);
                let sps = sps.clone();
                let pps = pps.clone();
                let mut sps_bytes = vec![];
                sps_bytes.extend_from_slice(ctx.user_context.sps_bytes(sps.seq_parameter_set_id).unwrap());
                let mut pps_bytes = vec![];
                pps_bytes.extend_from_slice(ctx.user_context.pps_bytes(pps.seq_parameter_set_id).unwrap());
                ctx.user_context.add_slice(
                    current_slice.header,
                    header,
                    sps,
                    pps,
                    sps_bytes,
                    pps_bytes,
                    current_slice.buf
                );
            },
            Err(e) => println!("slice_header() error: SliceHeaderError::{:?}", e),
        }
    }
}

#[derive(Default)]
struct NalCapture {
    buf: Vec<u8>,
}
impl NalHandler for NalCapture {
    type Ctx = IngestH264Context;

    fn start(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, header: NalHeader) {
        self.buf.clear();
    }

    fn push(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, buf: &[u8]) {
        self.buf.extend_from_slice(buf);
    }

    fn end(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>) {
    }
}

struct DateTime {
    date: u64,
    time_of_day_micros: u64,
}
impl DateTime {
    const DAY_SECONDS: u64 = 24 * 60 * 60;
    const DAY_MICROS: u64 = Self::DAY_SECONDS * 1_000_000;

    pub fn now() -> Result<DateTime, SystemTimeError> {
        // TODO: as usual, leap-seconds mess everything up; consider option for pic_timing to align with PTP time
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
        Ok(DateTime {
            date: now.as_secs() / Self::DAY_SECONDS,
            time_of_day_micros: (now.as_secs() % Self::DAY_SECONDS) * 1_000_000 + now.subsec_micros() as u64,
        })
    }
}

struct PicTimingIngest {
    local_time_datum: Option<DateTime>,
    /// in 90-thousandths of a second (mpegts timebase)
    pts_to_utc_offset: i64,
}
impl Default for PicTimingIngest {
    fn default() -> Self {
        PicTimingIngest {
            local_time_datum: None,
            pts_to_utc_offset: 0,
        }
    }
}
impl PicTimingIngest {
    pub fn adjust_pts_to_utc(&self, pts_dts: i64) -> i64 {
        self.pts_to_utc_offset + pts_dts
    }
}
impl nal::sei::pic_timing::PicTimingHandler for PicTimingIngest {
    type Ctx = IngestH264Context;

    fn handle(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, pic_timing: nal::sei::pic_timing::PicTiming) {
        // Ordering: this depends on the PTS for this frame having already been
        // placed into the context by H264ElementaryStreamConsumer
        const FRAME_RATE: u64 = 25;
        if let Some(pic_struct) = pic_timing.pic_struct {
            if !pic_struct.clock_timestamps.is_empty() {
                if let Some(ref clock_timestamp) = pic_struct.clock_timestamps[0] {
                    let time_of_day_micros = (
                        (
                            (
                                (
                                    clock_timestamp.smh.hours() as u64
                                ) * 60 + clock_timestamp.smh.minutes() as u64
                            ) * 60 + clock_timestamp.smh.seconds() as u64
                        ) * FRAME_RATE + clock_timestamp.n_frames as u64
                    ) * 1_000_000 / FRAME_RATE;

                    let date_time = DateTime::now().unwrap(/*TODO*/);
                    let time_diff = date_time.time_of_day_micros as i64 - time_of_day_micros as i64;
                    let date_part = if time_diff < -(DateTime::DAY_MICROS as i64 / 2) {
                        date_time.date + 1
                    } else if time_diff > DateTime::DAY_MICROS as i64 / 2 {
                        date_time.date - 1
                    } else {
                        date_time.date
                    };
                    if let Some(pts) = ctx.user_context.last_pts.or_else(|| ctx.user_context.last_dts ) {
                        // Convert local time to the media timebase (going the other way around
                        // might risk introducing a small amount of error into results)
                        let local_converted = date_time.date * DateTime::DAY_SECONDS * Timestamp::TIMEBASE
                            + time_of_day_micros * Timestamp::TIMEBASE / 1_000_000;
                        let pts_to_datetime = local_converted as i64 - pts;
                        ctx.user_context.store.set_pts_to_utc(pts_to_datetime);
                        //println!("pts_to_datetime {}", pts_to_datetime);
                    }
                }
            }
        }
    }
}
h264_reader::sei_switch!{
    SeiSwitch<IngestH264Context> {
        //BufferingPeriod: h264_reader::nal::sei::buffering_period::BufferingPeriodPayloadReader
        //    => h264_reader::nal::sei::buffering_period::BufferingPeriodPayloadReader::new(),
        //UserDataRegisteredItuTT35: h264_reader::nal::sei::user_data_registered_itu_t_t35::UserDataRegisteredItuTT35Reader<TT35Switch>
        //    => h264_reader::nal::sei::user_data_registered_itu_t_t35::UserDataRegisteredItuTT35Reader::new(TT35Switch::default()),
        PicTiming: h264_reader::nal::sei::pic_timing::PicTimingReader<PicTimingIngest>
            => h264_reader::nal::sei::pic_timing::PicTimingReader::new(PicTimingIngest::default()),
    }
}
struct IngestSeiPayoadReader {
    switch: SeiSwitch,
}
impl h264_reader::nal::sei::SeiIncrementalPayloadReader for IngestSeiPayoadReader {
    type Ctx = IngestH264Context;

    fn start(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, payload_type: h264_reader::nal::sei::HeaderType, payload_size: u32) {
        //println!("  SEI: {:?} size={}", payload_type, payload_size);
        self.switch.start(ctx, payload_type, payload_size)
    }

    fn push(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, buf: &[u8]) {
        self.switch.push(ctx, buf)
    }

    fn end(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>) {
        self.switch.end(ctx)
    }

    fn reset(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>) {
        self.switch.reset(ctx)
    }
}



struct IngestH264Context {
    store: store::Store,
    track_id: Option<store::TrackId>,
    last_pts: Option<i64>,
    last_dts: Option<i64>,
    sps_bytes: HashMap<nal::pps::ParamSetId, Vec<u8>>,
    pps_bytes: HashMap<nal::pps::ParamSetId, Vec<u8>>,
    max_bitrate: Option<u32>,
    unwrap_ts: super::UnwrapTimestamp,
}
impl IngestH264Context {
    fn new(store: store::Store, max_bitrate: Option<u32>) -> Self {
        IngestH264Context {
            store,
            track_id: None,
            last_pts: None,
            last_dts: None,
            sps_bytes: HashMap::new(),
            pps_bytes: HashMap::new(),
            max_bitrate,
            unwrap_ts: super::UnwrapTimestamp::default(),
        }
    }

    fn set_pts_dts(&mut self, pts: Option<pes::Timestamp>, dts: Option<pes::Timestamp>) {
        let (dts, pts) = if let Some(dts) = dts {
            self.unwrap_ts.update(dts);
            (
                Some(self.unwrap_ts.unwrap(dts)),
                pts.map(|pts| self.unwrap_ts.unwrap(pts)),
            )
        } else {
            if let Some(pts) = pts {
                self.unwrap_ts.update(pts);
                let pts = self.unwrap_ts.unwrap(pts);
                (
                    None,
                    Some(pts),
                )
            } else {
                (None, None)
            }
        };
        self.last_pts = pts;
        self.last_dts = dts;
    }

    fn add_slice(&mut self,
                 nal_header: nal::NalHeader,
                 slice_header: nal::slice::SliceHeader,
                 sps: nal::sps::SeqParameterSet,
                 pps: nal::pps::PicParameterSet,
                 sps_bytes: Vec<u8>,
                 pps_bytes: Vec<u8>,
                 slice_data: Vec<u8>,
    ) {
        let track_id = if let Some(tid) = self.track_id {
            tid
        } else {
            let tid = self.store.allocate_avc_track(sps, pps, sps_bytes, pps_bytes, self.max_bitrate);
            self.track_id = Some(tid);
            tid
        };
        let (dts, pts) = if let Some(dts) = self.last_dts {
            (
                dts,
                self.last_pts.unwrap_or(0),
            )
        } else {
            if let Some(pts) = self.last_pts {
                (
                    pts,
                    pts,
                )
            } else {
                (0, 0)
            }
        };
        self.store.add_avc_sample(track_id, store::Sample {
            header: store::SampleHeader::Avc(nal_header, slice_header),
            data: slice_data,
            pts,
            dts,
        });
    }

    pub fn sps_bytes(&self, sps_id: ParamSetId) -> Option<&[u8]> {
        self.sps_bytes.get(&sps_id).map(|v| &v[..] )
    }

    pub fn pps_bytes(&self, pps_id: ParamSetId) -> Option<&[u8]> {
        self.pps_bytes.get(&pps_id).map(|v| &v[..] )
    }
}

#[derive(Default)]
struct SpsIngestNalHandler {
    buf: Vec<u8>,
    header: Option<NalHeader>,
}
impl NalHandler for SpsIngestNalHandler {
    type Ctx = IngestH264Context;

    fn start(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, header: NalHeader) {
        assert_eq!(header.nal_unit_type(), nal::UnitType::SeqParameterSet);
        self.buf.clear();
        self.buf.push(header.into());
        self.header = Some(header);
    }

    fn push(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, buf: &[u8]) {
        self.buf.extend_from_slice(buf);
    }

    fn end(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>) {
        let capture = NalCapture::default();
        let mut decode = RbspDecoder::new(capture);
        decode.start(ctx, self.header.unwrap());
        decode.push(ctx, &self.buf[1..]);
        decode.end(ctx);
        let capture = decode.into_handler();
        let sps = nal::sps::SeqParameterSet::from_bytes(&capture.buf[..]);
        if let Ok(sps) = sps {
            ctx.user_context.sps_bytes.insert(sps.seq_parameter_set_id, self.buf.clone());
            ctx.put_seq_param_set(sps);
        }
        self.buf.clear();
        self.header = None;
    }
}
#[derive(Default)]
struct PpsIngestNalHandler {
    buf: Vec<u8>,
    header: Option<NalHeader>,
}
impl NalHandler for PpsIngestNalHandler {
    type Ctx = IngestH264Context;

    fn start(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, header: NalHeader) {
        assert_eq!(header.nal_unit_type(), nal::UnitType::PicParameterSet);
        self.buf.clear();
        self.buf.push(header.into());
        self.header = Some(header);
    }

    fn push(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>, buf: &[u8]) {
        self.buf.extend_from_slice(buf);
    }

    fn end(&mut self, ctx: &mut h264_reader::Context<Self::Ctx>) {
        let capture = NalCapture::default();
        let mut decode = RbspDecoder::new(capture);
        decode.start(ctx, self.header.unwrap());
        decode.push(ctx, &self.buf[1..]);
        decode.end(ctx);
        let capture = decode.into_handler();
        let pps = nal::pps::PicParameterSet::from_bytes(ctx, &capture.buf[..]);
        if let Ok(pps) = pps {
            ctx.user_context.pps_bytes.insert(pps.pic_parameter_set_id, self.buf.clone());
            ctx.put_pic_param_set(pps);
        }
        self.buf.clear();
        self.header = None;
    }
}
pub struct H264ElementaryStreamConsumer {
    pid: packet::Pid,
    ctx: h264_reader::Context<IngestH264Context>,
    parser: h264_reader::annexb::AnnexBReader<h264_reader::nal::NalSwitch<IngestH264Context>, IngestH264Context>,
}
impl H264ElementaryStreamConsumer {
    pub fn construct(stream_info: &psi::pmt::StreamInfo, store: store::Store) -> pes::PesPacketFilter<IngestDemuxContext, H264ElementaryStreamConsumer> {
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
                    _ => println!("  H264 {:?}: {:?}", stream_info.elementary_pid(), d),
                }
                Err(e) => println!("  H264 {:?}: Error reading descriptor: {:?}", stream_info.elementary_pid(), e),
            }
        }
        let ctx = IngestH264Context::new(store, max_bitrate);
        let mut switch = h264_reader::nal::NalSwitch::new();
        let sei_handler = h264_reader::nal::sei::SeiNalHandler::new(IngestSeiPayoadReader { switch: SeiSwitch::default() });
        let sps_handler = SpsIngestNalHandler::default();
        let pps_handler = PpsIngestNalHandler::default();
        let slice_wout_part_idr_handler = SliceIngest::new(SliceType::Idr);
        let slice_wout_part_nonidr_handler = SliceIngest::new(SliceType::NonIdr);
        switch.put_handler(h264_reader::nal::UnitType::SEI, Box::new(RefCell::new(sei_handler)));
        switch.put_handler(h264_reader::nal::UnitType::SeqParameterSet, Box::new(RefCell::new(sps_handler)));
        switch.put_handler(h264_reader::nal::UnitType::PicParameterSet, Box::new(RefCell::new(pps_handler)));
        switch.put_handler(h264_reader::nal::UnitType::SliceLayerWithoutPartitioningIdr, Box::new(RefCell::new(slice_wout_part_idr_handler)));
        switch.put_handler(h264_reader::nal::UnitType::SliceLayerWithoutPartitioningNonIdr, Box::new(RefCell::new(slice_wout_part_nonidr_handler)));
        pes::PesPacketFilter::new(
            H264ElementaryStreamConsumer {
                pid: stream_info.elementary_pid(),
                ctx: h264_reader::Context::new(ctx),
                parser: h264_reader::annexb::AnnexBReader::new(switch)
            }
        )
    }
}
impl pes::ElementaryStreamConsumer for H264ElementaryStreamConsumer {
    fn start_stream(&mut self) {
        println!("H264 start_steam()");
    }
    fn begin_packet(&mut self, header: pes::PesHeader) {
        match header.contents() {
            pes::PesContents::Parsed(Some(parsed)) => {
                // TODO: make note of es_rate settings if present to drive HLS Master Manifest metadata
                match parsed.pts_dts() {
                    Ok(pes::PtsDts::PtsOnly(Ok(pts))) => self.ctx.user_context.set_pts_dts(Some(pts), None),
                    Ok(pes::PtsDts::Both{pts:Ok(pts), dts:Ok(dts)}) => self.ctx.user_context.set_pts_dts(Some(pts), Some(dts)),
                    _ => self.ctx.user_context.set_pts_dts(None, None),
                }
                if parsed.data_alignment_indicator() ==pes::DataAlignment::Aligned {
                    self.parser.start(&mut self.ctx);
                } else {
                    self.parser.start(&mut self.ctx);
                }
                self.parser.push(&mut self.ctx, parsed.payload());
            },
            pes::PesContents::Parsed(None) => println!("H264: Parsed(None)"),
            pes::PesContents::Payload(payload) => {
                println!("H264 {:?} payload", self.pid);
                self.parser.start(&mut self.ctx);
                self.parser.push(&mut self.ctx, payload);
            },
        }
    }
    fn continue_packet(&mut self, data: &[u8]) {
        self.parser.push(&mut self.ctx, data);
    }
    fn end_packet(&mut self) {
        // TODO: at some point I had missed out this call to end_units() and the resulting problem
        // was hard to debug -- can the API be changed to make that kind of error either less
        // likely, or the failures easier to understand?
        self.parser.end_units(&mut self.ctx)
    }
    fn continuity_error(&mut self) {
        // TODO: self.parser.reset(ctx);
    }
}
