use std::collections::vec_deque::VecDeque;
use std::sync::{Mutex, MutexGuard, Arc};
use h264_reader::nal;
use std::collections::vec_deque;
use std::iter::Peekable;
use h264_reader::nal::UnitType;
use itertools::Itertools;
use tokio_sync::watch;

pub const SEG_DURATION_PTS: u64 = 172800;

const ARCHIVE_LIMIT: u64 = 60 * 60 * 90000;  // 1 hour

pub struct Sample {
    pub data: Vec<u8>,
    pub pts: i64,
    pub dts: i64,
    pub header: SampleHeader,
}

pub enum SampleHeader {
    Avc(nal::NalHeader, nal::slice::SliceHeader),
    Aac,
}

#[derive(Debug)]
pub enum SegmentError {
    BadSampleTime(i64),
    /// Tried to inspect segment information, but no segments exist within the track (yet)
    NoSegments,
    /// Tried to inspect the parts for a segment, but the segment does not have any parts (hls
    /// says only the very most recent segments should present parts)
    NoPartsForSegment,
}

/// Notification value used to describe updates to a track in the face of media being ingested
#[derive(Default, Debug, Clone, Copy)]
pub struct TrackSequence {
    pub seg: u64,
    pub part: u16,
}

pub struct AvcTrack {
    sps: nal::sps::SeqParameterSet,
    pps: nal::pps::PicParameterSet,
    sps_bytes: Vec<u8>,
    pps_bytes: Vec<u8>,
    samples: VecDeque<Sample>,
    max_bitrate: Option<u32>,
    watch: (watch::Sender<TrackSequence>, watch::Receiver<TrackSequence>),
    first_seg_num: usize,
}
impl AvcTrack {
    fn new(
        sps: nal::sps::SeqParameterSet,
        pps: nal::pps::PicParameterSet,
        sps_bytes: Vec<u8>,
        pps_bytes: Vec<u8>,
        max_bitrate: Option<u32>
    ) -> AvcTrack {
        AvcTrack {
            sps,
            pps,
            sps_bytes,
            pps_bytes,
            samples: VecDeque::new(),
            max_bitrate,
            watch: watch::channel(TrackSequence::default()),
            first_seg_num: 0,
        }
    }

    pub fn push(&mut self, sample: Sample) {
        self.samples.push_back(sample);
        // TODO: pretty inefficient!
        if let Some((this_msn, this_seg)) = self.segments().enumerate().last() {
            let this_part = self.parts(this_seg.id()).unwrap().count() - 1;
            let seq = TrackSequence {
                seg: this_msn as u64 + self.first_seg_num as u64,
                part: this_part as u16,
            };
            self.watch.0.broadcast(seq).unwrap()
        }
        while self.duration() > ARCHIVE_LIMIT {
            self.remove_one_segment();
        }
    }
    fn remove_one_segment(&mut self) {
        let mut i = 0;
        while i == 0 || !is_idr(&self.samples[0]) {
            self.samples.pop_front();
            i += 1;
        }
        self.first_seg_num += 1;
    }
    fn duration(&self) -> u64 {
        let len = self.samples.len();
        if len < 2 {
            0
        } else {
            (self.samples[len - 1].dts - self.samples[0].dts) as u64
        }
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

    pub fn segment_number_for(&self, dts: i64) -> Option<usize> {
        // TODO: assert first sample dts exactly equals given value, and that it is_idr()
        self.segments()
            .enumerate()
            .find(|(i, seg)| seg.dts == dts)
            .map(|(i, _)| i + self.first_seg_num )

    }

    pub fn part_number_for(&self, dts: i64, part_id: u64) -> Option<usize> {
        Some(self.segments()
            .take_while(|seg| seg.dts <= dts)
            .flat_map(|seg| {
                let limit = if seg.dts == dts {
                    part_id
                } else {
                    std::u64::MAX
                };
                self.parts(seg.id()).unwrap(/*TODO*/)
                    .take_while(move |part| part.id() <= limit)
            })
            .count())
    }

    pub fn segment_samples(&self, dts: i64) -> Result<impl Iterator<Item = &Sample>, SegmentError> {
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

    pub fn sequence_stream(&self) -> watch::Receiver<TrackSequence> {
        self.watch.1.clone()
    }

    pub fn sample(&self, dts: i64) -> Option<&Sample> {
        self.samples
            .iter()
            .find(|sample| sample.dts == dts )
    }

    pub fn bandwidth(&self) -> Option<u32> {
        // TODO: measure max bandwidth per GOP, and report here
        self.max_bitrate
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
        AvcSegmentIterator{
            samples: self.samples.iter().peekable(),
            max_ts: self.samples.iter().last().map(|s| s.dts ),
            sequence_number: self.first_seg_num as u64,
        }
    }

    pub fn media_sequence_number(&self) -> u64 {
        self.segments().count() as u64
    }

    // TODO: this should be,
    //  a) in terms of duration, not samples
    //  b) configured, not hardcoded
    pub const VIDEO_SAMPLES_PER_PART: usize = 8;

    pub fn has_parts(&self, dts: i64) -> bool {
        let latest = self.samples.iter().last().map(|s| s.dts );
        if latest.is_none() {
            return false
        }
        let latest = latest.unwrap();
        let earliest_segment_with_parts = latest - (SEG_DURATION_PTS * 3) as i64;
        dts >= earliest_segment_with_parts
    }

    pub fn parts<'track>(&'track self, dts: i64) -> Result<impl Iterator<Item = PartInfo> + 'track, SegmentError> {
        Ok(self.segment_samples(dts)?
            .enumerate()
            .group_by(|(i, _)| i / Self::VIDEO_SAMPLES_PER_PART )
            .into_iter()
            .map(|(key, group)| {  // TODO: can we avoid allocating for 'group'?
                // now, check that we have all the samples needed for a complete part, and remember
                // if there's an IDR frame, so that the INDEPENDENT flag can be set in the HLS
                // media-manifest
                let (count, idr) = group
                    .iter()
                    .map(|(_, s)| s )
                    .fold((0, false), |(count, idr), sample| (count+1, idr | is_idr(sample)) );
                if count == Self::VIDEO_SAMPLES_PER_PART {
                    Some(PartInfo {
                        part_id: key as u64,
                        duration: Some(0.32),  // TODO: don't hardcode
                        continuous: true,
                        independent: idr,
                    })
                } else {
                    None
                }
            })
            .flat_map(|x| x))
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

struct AvcPartIterator<'track> {
    samples: Peekable<vec_deque::Iter<'track, Sample>>,
}

struct AvcSegmentIterator<'track> {
    samples: Peekable<vec_deque::Iter<'track, Sample>>,
    max_ts: Option<i64>,
    sequence_number: u64,
}
impl<'track> Iterator for AvcSegmentIterator<'track> {
    type Item = SegmentInfo;

    fn next(&mut self) -> Option<Self::Item> {
        if self.max_ts.is_none() {
            return None;
        }
        let max_ts = self.max_ts.unwrap();

        let mut skipped = false;
        let mut discontinuity = false;
        let mut dts = None;
        loop {
            match self.samples.next() {
                Some(sample) => {
                    if is_idr(sample) {
                        dts = Some(sample.dts);
                    }

                    if let Some(last_idr_dts) = dts {
                        match self.samples.peek() {
                            Some(peek) => {
                                if is_idr(peek) {
                                    let duration = peek.dts - last_idr_dts;
                                    let seq = self.sequence_number;
                                    self.sequence_number += 1;
                                    return Some(SegmentInfo {
                                        dts: last_idr_dts,
                                        seq,
                                        duration: Some(duration as f64 / 90000.0),
                                        continuous: true
                                    })
                                }
                            },
                            // Then we don't have enough samples to announce this segment yet;
                            // we do indicate the possibility of a segment, but we don't indicate
                            // it's duration yet,
                            None => {
                                let seq = self.sequence_number;
                                self.sequence_number += 1;
                                return Some(SegmentInfo {
                                    dts: last_idr_dts,
                                    seq,
                                    duration: None,
                                    continuous: true
                                })
                            }
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
    max_bitrate: Option<u32>,
    watch: (watch::Sender<TrackSequence>, watch::Receiver<TrackSequence>),
    first_seg_num: usize,
}
impl AacTrack {
    pub const AUDIO_FRAMES_PER_PART: usize = 15;  // TODO

    fn new(
        profile: adts_reader::AudioObjectType,
        frequency: adts_reader::SamplingFrequency,
        channel_config: adts_reader::ChannelConfiguration,
        max_bitrate: Option<u32>,
    ) -> AacTrack {
        AacTrack {
            samples: VecDeque::new(),
            profile,
            frequency,
            channel_config,
            max_bitrate,
            watch: watch::channel(TrackSequence::default()),
            first_seg_num: 0,
        }
    }

    pub fn push(&mut self, sample: Sample) {
        self.samples.push_back(sample);
        while self.duration() > ARCHIVE_LIMIT {
            self.remove_one_segment()
        }
        // TODO: pretty inefficient!
        if let Some((this_msn, this_seg)) = self.segments().enumerate().last() {
            let this_part = self.parts(this_seg.id()).unwrap().count() - 1;
            let seq = TrackSequence {
                seg: this_msn as u64 + self.first_seg_num as u64,
                part: this_part as u16,
            };
            self.watch.0.broadcast(seq).unwrap()
        }
    }

    fn remove_one_segment(&mut self) {
        for _ in 0..Self::AAC_SAMPLES_PER_SEGMENT {
            self.samples.pop_front();
        }
        self.first_seg_num += 1;
    }

    fn duration(&self) -> u64 {
        let len = self.samples.len();
        if len < 2 {
            0
        } else {
            (self.samples[len - 1].dts - self.samples[0].dts) as u64
        }
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

    fn latest_dts(&self) -> Result<i64, SegmentError> {
        let latest = self.samples.iter().last().map(|s| s.dts );
        if latest.is_none() {
            return Err(SegmentError::NoSegments)
        }
        Ok(latest.unwrap())
    }

    pub fn has_parts(&self, dts: i64) -> bool {
        let latest = match self.latest_dts() {
            Ok(latest) => latest,
            Err(_) => return false,
        };
        let earliest_segment_with_parts = latest - (SEG_DURATION_PTS * 3) as i64;
        dts >= earliest_segment_with_parts
    }

    pub fn parts<'track>(&'track self, dts: i64) -> Result<impl Iterator<Item = PartInfo> + 'track, SegmentError> {
        // TODO: this should be,
        //  a) in terms of duration, not samples
        //  b) configured, not hardcoded
        const AUDIO_SAMPLES_PER_PART: usize = 15;


        Ok(self.segment_samples(dts)
            .enumerate()
            .group_by(|(i, _)| i / AUDIO_SAMPLES_PER_PART )
            .into_iter()
            .map(|(key, group)| {  // TODO: can we avoid allocating for 'group'?
                // now, check that we have all the samples needed for a complete part
                if group.len() == AUDIO_SAMPLES_PER_PART {
                    Some(PartInfo {
                        part_id: key as u64,
                        duration: Some(0.32),  // TODO: don't hardcode
                        continuous: true,
                        independent: false,  // arguably could be true, and an audio media-manifest just ignores?
                    })
                } else {
                    None
                }
            })
            .flat_map(|x| x))
    }

    pub fn segment_number_for(&self, dts: i64) -> Option<usize> {
        // TODO: assert first sample dts exactly equals given value, and that it is_idr()
        self.segments()
            .enumerate()
            .find(|(i, seg)| seg.dts == dts)
            .map(|(i, _)| i + self.first_seg_num )
    }

    pub fn part_number_for(&self, dts: i64, part_id: u64) -> Option<usize> {
        Some(self.segments()
            .take_while(|seg| seg.dts <= dts)
            .flat_map(|seg| {
                let limit = if seg.dts == dts {
                    part_id
                } else {
                    std::u64::MAX
                };
                self.parts(seg.id()).unwrap(/*TODO*/)
                    .take_while(move |part| part.id() <= limit)
            })
            .count())
    }

    const AAC_SAMPLES_PER_SEGMENT: usize = 90;  // TODO: can't be hardcoded

    pub fn segments<'track>(&'track self) -> impl Iterator<Item = SegmentInfo> + 'track {
        const AAC_SEGMENT_DURATION: f64 = 1.92;  // TODO: can't be hardcoded

        // apply a limit so as to avoid the last segment being announced while incomplete
        // TODO: expose partial segment (duration:None)
        let limit = self.samples.len() / Self::AAC_SAMPLES_PER_SEGMENT;

        let seg_num = self.first_seg_num;
        self.samples
            .iter()
            .enumerate()
            .group_by(|(i, _)| i / Self::AAC_SAMPLES_PER_SEGMENT )
            .into_iter()
            .map(move |(key, group)| {  // TODO: can we avoid allocating for 'group'?
                if group.len() == Self::AAC_SAMPLES_PER_SEGMENT {
                    SegmentInfo {
                        dts: group[0].1.dts,
                        seq: key as u64 + seg_num as u64,
                        duration: Some(AAC_SEGMENT_DURATION),
                        continuous: true, // TODO check for timing gaps etc.
                    }
                } else {
                    SegmentInfo {
                        dts: group[0].1.dts,
                        seq: key as u64 + seg_num as u64,
                        duration: None,
                        continuous: true, // TODO check for timing gaps etc.
                    }
                }
            })
    }

    pub fn media_sequence_number(&self) -> u64 {
        self.segments().count() as u64
    }

    pub fn sample(&self, dts: i64) -> Option<&Sample> {
        self.samples
            .iter()
            .find(|sample| sample.dts == dts )
    }

    pub fn segment_samples(&self, dts: i64) -> impl Iterator<Item = &Sample> {
        // TODO: assert first sample dts exactly equals given value, and that it is_idr()
        self.samples()
            .skip_while(move |sample| sample.dts < dts )
            .take(Self::AAC_SAMPLES_PER_SEGMENT)
    }

    pub fn sequence_stream(&self) -> watch::Receiver<TrackSequence> {
        self.watch.1.clone()
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
    dts: i64,
    seq: u64,
    duration: Option<f64>,
    continuous: bool,
}
impl SegmentInfo {
    pub fn id(&self) -> i64 {
        self.dts
    }
    pub fn duration_seconds(&self) -> Option<f64> {
        self.duration
    }
    pub fn is_continuous(&self) -> bool {
        self.continuous
    }
    pub fn sequence_number(&self) -> u64 {
        self.seq
    }
}

pub struct PartInfo {
    part_id: u64,
    duration: Option<f64>,
    continuous: bool,
    independent: bool,
}
impl PartInfo {
    pub fn id(&self) -> u64 {
        self.part_id
    }
    pub fn duration_seconds(&self) -> Option<f64> {
        self.duration
    }
    pub fn is_continuous(&self) -> bool {
        self.continuous
    }
    pub fn is_independent(&self) -> bool {
        self.independent
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
        max_bitrate: Option<u32>
    ) -> TrackId {
        let mut state = self.get_state_mut();
        let track = AvcTrack::new(sps, pps, sps_bytes, pps_bytes, max_bitrate);
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
        max_bitrate: Option<u32>,
    ) -> TrackId {
        let mut state = self.get_state_mut();
        let track = AacTrack::new(profile, frequency, channel_config, max_bitrate);
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