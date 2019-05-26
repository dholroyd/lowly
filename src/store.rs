use std::collections::vec_deque::VecDeque;
use std::sync::{Mutex, MutexGuard, Arc};
use h264_reader::nal;
use std::collections::vec_deque;
use std::iter::Peekable;
use h264_reader::nal::UnitType;

pub struct Sample {
    pub data: Vec<u8>,
    pub pts: u64,
    pub dts: u64,
    pub header: SampleHeader,
}

pub enum SampleHeader {
    Avc(nal::NalHeader, nal::slice::SliceHeader),
}

pub struct AvcTrack {
    sps: nal::sps::SeqParameterSet,
    pps: nal::pps::PicParameterSet,
    sps_bytes: Vec<u8>,
    pps_bytes: Vec<u8>,
    samples: VecDeque<Sample>,
}
impl AvcTrack {
    fn new(
        sps: nal::sps::SeqParameterSet,
        pps: nal::pps::PicParameterSet,
        sps_bytes: Vec<u8>,
        pps_bytes: Vec<u8>,
    ) -> AvcTrack {
        AvcTrack {
            sps,
            pps,
            sps_bytes,
            pps_bytes,
            samples: VecDeque::new(),
        }
    }

    pub fn push(&mut self, sample: Sample) {
        self.samples.push_back(sample);
        // TODO: remove old samples
    }
    pub fn pps(&self) -> &h264_reader::nal::pps::PicParameterSet {
        &self.pps
    }
    pub fn sps(&self) -> &h264_reader::nal::sps::SeqParameterSet {
        &self.sps
    }

    pub fn samples(&self) -> impl Iterator<Item = &Sample> {
        self.samples.iter()
    }

    pub fn segment_number_for(&self, dts: u64) -> Option<usize> {
        // TODO: assert first sample dts exactly equals given value, and that it is_idr()
        self.segments()
            .enumerate()
            .find(|(i, seg)| seg.dts == dts)
            .map(|(i, _)| i )
    }

    pub fn segment_samples(&self, dts: u64) -> impl Iterator<Item = &Sample> {
        // TODO: assert first sample dts exactly equals given value, and that it is_idr()
        self.samples()
            .skip_while(move |sample| sample.dts < dts )
            .enumerate()
            .take_while(|(i, sample)| *i==0 || !is_idr(sample) )
            .map(|(i, sample)| sample )
    }

    pub fn sample(&self, dts: u64) -> Option<&Sample> {
        self.samples
            .iter()
            .find(|sample| sample.dts == dts )
    }

    pub fn bandwidth(&self) -> u32 {
        // TODO: measure max bandwidth per GOP, and report here
        283000
    }

    pub fn rfc6381_codec(&self) -> String {
        let mut bytes = make_avc_codec_bytes(&self.sps);
        format!("avc1.{:02x}{:02x}{:02x}", bytes[0], bytes[1], bytes[2])
    }

    pub fn max_chunk_duration(&self) -> u32 {
        // TODO: measure GOP duration during ingest, and report here
        //       also, consider proper time bases etc
        2
    }
    pub fn segments<'track>(&'track self) -> impl Iterator<Item = SegmentInfo> + 'track {
        AvcSegmentIterator(self.samples.iter().peekable())
    }
    pub fn sps_bytes(&self) -> &[u8] {
        &self.sps_bytes[..]
    }
    pub fn pps_bytes(&self) -> &[u8] {
        &self.pps_bytes[..]
    }

    pub fn dimensions(&self) -> (u32, u32) {
        let sps = &self.sps;
        let width = (sps.pic_width_in_mbs_minus1 + 1) * 16;
        let mul = match sps.frame_mbs_flags {
            nal::sps::FrameMbsFlags::Fields { .. } => 2,
            nal::sps::FrameMbsFlags::Frames => 1,
        };
        let height = mul * (sps.pic_height_in_map_units_minus1 + 1) * 16;
        (width, height)
    }
}

struct AvcSegmentIterator<'track>(Peekable<vec_deque::Iter<'track, Sample>>);
impl<'track> Iterator for AvcSegmentIterator<'track> {
    type Item = SegmentInfo;

    fn next(&mut self) -> Option<Self::Item> {
        let mut skipped = false;
        let mut discontinuity = false;
        let mut dts = None;
        loop {
            match self.0.next() {
                Some(sample) => {
                    if is_idr(sample) {
                        dts = Some(sample.dts);
                    }

                    match self.0.peek() {
                        Some(peek) => {
                            if is_idr(peek) {
                                let duration = peek.dts - dts.unwrap();
                                return Some(SegmentInfo {
                                    dts: dts.unwrap(),
                                    duration: duration as f64 / 90000.0,
                                    continuous: true
                                })
                            }

                        },
                        // Then we don't have enough samples to announce this segment yet; maybe
                        // next time.  Problem if the stream ended though, since we will not make
                        // those final samples available.  Need explicit EOS signal?
                        None => return None,
                    }
                },
                None => return None,
            }
        }
    }
}

fn is_idr(sample: &Sample) -> bool {
    match sample.header {
        SampleHeader::Avc(nal_header, _) => {
            nal_header.nal_unit_type() == UnitType::SliceLayerWithoutPartitioningIdr
        },
        _ => false,
    }
}

pub struct SegmentInfo {
    dts: u64,
    duration: f64,
    continuous: bool,
}
impl SegmentInfo {
    pub fn id(&self) -> u64 {
        self.dts
    }
    pub fn duration_seconds(&self) -> f64 {
        self.duration
    }
    pub fn is_continuous(&self) -> bool {
        self.continuous
    }
}

fn make_avc_codec_bytes(sps: &nal::sps::SeqParameterSet) -> [u8; 3] {
    let flags = sps.constraint_flags
        .iter()
        .enumerate()
        .fold(0, |acc, (i, f)| acc | if *f { 1 << i } else { 0 } );
    [
        sps.profile_idc.into(),
        flags,
        sps.level_idc
    ]
}
pub enum Track {
    Avc(AvcTrack)
}

#[derive(Default)]
struct State {
    tracks: Vec<Track>,
}

pub struct TrackInfo {
    pub track_id: TrackId,
}
pub struct TrackRef<'store> {
    state: MutexGuard<'store, State>,
    track_id: TrackId,
}
impl<'store> TrackRef<'store> {
    pub fn id(&self) -> TrackId {
        self.track_id
    }

    pub fn track(&mut self) -> &Track {
        &self.state.tracks[self.track_id.0]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TrackId(pub usize);

#[derive(Clone)]
pub struct Store {
    // Mutex is a blunt tool, but gets us going more quickly.  Revisit this once performance
    // profiles show were this needs to be sped up.
    state: Arc<Mutex<State>>,
}
impl Store {
    pub fn new() -> Store {
        Store {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn get_state_mut(&mut self) -> MutexGuard<State> {
        self.state.lock().unwrap()
    }

    pub fn allocate_avc_track(
        &mut self,
        sps: nal::sps::SeqParameterSet,
        pps: nal::pps::PicParameterSet,
        sps_bytes: Vec<u8>,
        pps_bytes: Vec<u8>,
    ) -> TrackId {
        let mut state = self.get_state_mut();
        let track = AvcTrack::new(sps, pps, sps_bytes, pps_bytes);
        let id = TrackId(state.tracks.len());
        state.tracks.push(Track::Avc(track));
        id
    }

    pub fn add_avc_sample(&mut self, track_id: TrackId, sample: Sample) {
        let mut state = self.get_state_mut();
        if let Track::Avc(ref mut track) = state.tracks[track_id.0] {
            track.push(sample);
        } else {
            panic!("Not an AVC track {:?}", track_id)
        }
    }

    pub fn track_list(&mut self) -> impl Iterator<Item = TrackInfo> {
        let state = self.get_state_mut();
        state.tracks
            .iter()
            .enumerate()
            .map(|(index, track)| TrackInfo { track_id: TrackId(index) } )
            .collect::<Vec<TrackInfo>>()
            .into_iter()
    }

    pub fn get_track<'store>(&mut self, track_id: TrackId) -> Option<TrackRef> {
        let state = self.get_state_mut();
        if track_id.0 >= state.tracks.len() {
            None
        } else {
            Some(TrackRef{ state, track_id })
        }
    }
}