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
    Aac,
}

#[derive(Debug)]
pub enum SegmentError {
    BadSampleTime(u64)
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

    pub fn segment_samples(&self, dts: u64) -> Result<impl Iterator<Item = &Sample>, SegmentError> {
        let mut iter = self.samples()
            .skip_while(move |sample| sample.dts < dts )
            .enumerate()
            .take_while(|(i, sample)| *i==0 || !is_idr(sample) )
            .map(|(i, sample)| sample )
            .peekable();
        if let Some(sample) = iter.peek() {
            if sample.dts == dts && is_idr(sample) {
                Ok(iter)
            } else {
                Err(SegmentError::BadSampleTime(dts))
            }
        } else {
            Err(SegmentError::BadSampleTime(dts))
        }
    }

    pub fn sample(&self, dts: u64) -> Option<&Sample> {
        self.samples
            .iter()
            .find(|sample| sample.dts == dts )
    }

    pub fn bandwidth(&self) -> u32 {
        // TODO: measure max bandwidth per GOP, and report here
        112000
    }

    pub fn rfc6381_codec(&self) -> String {
        let bytes = make_avc_codec_bytes(&self.sps);
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
        let vsub = if sps.chroma_info.chroma_format == nal::sps::ChromaFormat::YUV420 { 1 } else { 0 };
        let hsub = if sps.chroma_info.chroma_format == nal::sps::ChromaFormat::YUV420 || sps.chroma_info.chroma_format == nal::sps::ChromaFormat::YUV422 { 1 } else { 0 };
        let step_x = 1 << hsub;
        let step_y = mul << vsub;

        let height = mul * (sps.pic_height_in_map_units_minus1 + 1) * 16;
        if let Some(ref crop) = sps.frame_cropping {
            (width - crop.left_offset * step_x - crop.right_offset * step_x, height - crop.top_offset * step_y - crop.bottom_offset * step_y)
        } else {
            (width, height)
        }
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

                    if dts.is_some() {
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

pub struct AacTrack {
    samples: VecDeque<Sample>,
    profile: adts_reader::AudioObjectType,
    frequency: adts_reader::SamplingFrequency,
    channel_config: adts_reader::ChannelConfiguration,
}
impl AacTrack {
    fn new(
        profile: adts_reader::AudioObjectType,
        frequency: adts_reader::SamplingFrequency,
        channel_config: adts_reader::ChannelConfiguration,
    ) -> AacTrack {
        AacTrack {
            samples: VecDeque::new(),
            profile,
            frequency,
            channel_config,
        }
    }

    pub fn push(&mut self, sample: Sample) {
        self.samples.push_back(sample);
        // TODO: remove old samples
    }

    pub fn channels(&self) -> Option<u32> {
        // TODO
        Some(2)
    }

    pub fn max_chunk_duration(&self) -> u32 {
        // TODO: some way of determining a sensible value ('similar' to video GOP duration / just
        //       a configured value?)
        2
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

    const AAC_SAMPLES_PER_SEGMENT: usize = 90;  // TODO: can't be hardcoded

    pub fn segments<'track>(&'track self) -> impl Iterator<Item = SegmentInfo> + 'track {
        const AAC_SEGMENT_DURATION: f64 = 1.92;  // TODO: can't be hardcoded

        // apply a limit so as to avoid the last segment being announced while incomplete
        let limit = self.samples.len() / Self::AAC_SAMPLES_PER_SEGMENT;

        self.samples
            .iter()
            .enumerate()
            .filter(|(i, s)| i % Self::AAC_SAMPLES_PER_SEGMENT == 0)
            .map(|(i, s)| SegmentInfo {
                dts: s.dts,
                duration: AAC_SEGMENT_DURATION,
                continuous: true, // TODO check for timing gaps etc.
            } )
            .take(limit)
    }

    pub fn sample(&self, dts: u64) -> Option<&Sample> {
        self.samples
            .iter()
            .find(|sample| sample.dts == dts )
    }

    pub fn segment_samples(&self, dts: u64) -> impl Iterator<Item = &Sample> {
        // TODO: assert first sample dts exactly equals given value, and that it is_idr()
        self.samples()
            .skip_while(move |sample| sample.dts < dts )
            .take(Self::AAC_SAMPLES_PER_SEGMENT)
    }

    pub fn profile(&self) -> adts_reader::AudioObjectType {
        self.profile
    }

    pub fn frequency(&self) -> adts_reader::SamplingFrequency {
        self.frequency
    }

    pub fn channel_config(&self) -> adts_reader::ChannelConfiguration {
        self.channel_config
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
    Avc(AvcTrack),
    Aac(AacTrack),
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

    pub fn add_aac_sample(&mut self, track_id: TrackId, sample: Sample) {
        let mut state = self.get_state_mut();
        if let Track::Aac(ref mut track) = state.tracks[track_id.0] {
            track.push(sample);
        } else {
            panic!("Not an AAC track {:?}", track_id)
        }
    }

    pub fn allocate_aac_track(
        &mut self,
        profile: adts_reader::AudioObjectType,
        frequency: adts_reader::SamplingFrequency,
        channel_config: adts_reader::ChannelConfiguration,
    ) -> TrackId {
        let mut state = self.get_state_mut();
        let track = AacTrack::new(profile, frequency, channel_config);
        let id = TrackId(state.tracks.len());
        state.tracks.push(Track::Aac(track));
        id
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