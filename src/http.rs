use hyper::{Body, Request, Response, Server, StatusCode, Uri};
use futures::future::{Future, Either};
use hyper::service::Service;
use crate::store;
use futures::future;
use std::{error, fmt};
use std::fmt::Display;
use std::fmt::Write as FmtWrite;
use std::io::Write as IoWrite;
use mse_fmp4::{fmp4, aac};
use mse_fmp4::io::WriteTo;
use mse_fmp4::fmp4::common::Mp4Box;
use mpeg2ts_reader::pes::Timestamp;
use byteorder::WriteBytesExt;
use url::Url;
use futures::stream::Stream;
use crate::store::SegmentError;
use chrono::offset::TimeZone;

type ImmediateFut = future::FutureResult<Response<Body>, HlsServiceError>;
type MediaManifestFut = Box<dyn Future<Item=Response<Body>, Error=HlsServiceError> + Send>;

struct HlsService {
    store: store::Store,
}
impl Service for HlsService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = HlsServiceError;
    type Future = Either<ImmediateFut, MediaManifestFut>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let path = req.uri().path();
        if path.starts_with("/master.m3u8") {
            Either::A(self.master_manifest(req))
        } else if path.starts_with("/track/") {
            let mut parts = path["/track/".len()..].splitn(2, "/");
            let id = parts.next();
            let rest = parts.next();
            if let Some(id) = id {
                let id = id.to_string();
                let rest = rest.map(|s| s.to_string() );
                self.track(req, id, rest)
            } else {
                Either::A(futures::future::ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Need a track id"))
                    .unwrap()))
            }
        } else {
            Either::A(futures::future::ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("I don't know that one"))
                .unwrap()))
        }
    }
}


impl HlsService {

    fn master_manifest(&mut self, req: Request<Body>) -> ImmediateFut {
        let mut text = String::new();
        writeln!(text, "#EXTM3U").unwrap();
        // TODO: validate correct version vs. used HSL features
        writeln!(text, "#EXT-X-VERSION:{}", 7).unwrap();
        writeln!(text, "#EXT-X-INDEPENDENT-SEGMENTS").unwrap();
        writeln!(text, "").unwrap();

        // TODO: audio, keyframes

        for track in self.store.track_list() {
            match self.store.get_track(track.track_id).unwrap().track() {
                store::Track::Avc(avc_track) => {
                    // TODO:
                    //  - FRAMERATE
                    let (width, height) = avc_track.dimensions();
                    write!(text, "#EXT-X-STREAM-INF:").unwrap();
                    if let Some(bandwidth) = avc_track.bandwidth() {
                        // BANDWITH is mandatory; but if we don't know it, what to do!
                        write!(text, "BANDWIDTH={},", bandwidth).unwrap();
                    }
                    writeln!(text,
                             "RESOLUTION={}x{},AUDIO=\"default-audio-group\"",
                             width,
                             height)
                        .unwrap();
                    writeln!(text, "track/{}/media.m3u8", track.track_id.0).unwrap();
                },
                store::Track::Aac(aac_track) => {
                    write!(text,
                             "#EXT-X-MEDIA:TYPE=AUDIO,URI=\"track/{}/media.m3u8\",GROUP-ID=\"default-audio-group\",LANGUAGE=\"es\",NAME=\"stream_9\",AUTOSELECT=YES",
                             track.track_id.0,
                    )
                    .unwrap();
                    if let Some(channels) = aac_track.channels() {
                        write!(text, ",CHANNELS=\"{}\"", channels)
                        .unwrap();
                    }
                    writeln!(text).unwrap();
                }
            }
        }
        futures::future::ok(Response::builder()
            .header("Content-Type", "application/vnd.apple.mpegurl")
            .header("Access-Control-Allow-Origin", "*")
            .body(Body::from(text))
            .unwrap())
    }

    fn track(&mut self, req: Request<Body>, track_id: String, rest: Option<String>) -> Either<ImmediateFut, MediaManifestFut> {
        if let Ok(id) = track_id.parse() {
            let track_id = store::TrackId(id);
            if self.store.get_track(track_id).is_none() {
                return Either::A(futures::future::ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("No such track"))
                    .unwrap()))
            }
            if let Some(rest) = rest {
                if "media.m3u8" == rest {
                    Self::media_manifest(req, &mut self.store, track_id)
                } else if "init.mp4" == rest {
                    Either::A(Self::initialisation_segment(req, self.store.get_track(track_id).unwrap()))
                } else if rest.starts_with("segment/") {
                    let mut parts = rest["segment/".len()..].splitn(2, "/");
                    let id = parts.next();
                    let rest = parts.next();
                    if let Some(id) = id {
                        let id = id.to_string();
                        let rest = rest.map(|s| s.to_string() );
                        Either::A(futures::future::ok(Self::fmp4_segment(req, self.store.get_track(track_id).unwrap(), id, rest)))
                    } else {
                        Either::A(futures::future::ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("Need a segment id"))
                            .unwrap()))
                    }
                } else {
                    Either::A(futures::future::ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Don't know how to do that to a track"))
                        .unwrap()))
                }
            } else {
                Either::A(futures::future::ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("What do you want to do with this track?"))
                    .unwrap()))
            }
        } else {
            Either::A(futures::future::ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Bad track id"))
                .unwrap()))
        }
    }

    fn hls_request_params(uri: &Uri) -> HlsRequest {
        // hack to turn the relative URL into a qualified one that Url::parse() will accept,
        let url = Url::parse(&format!("http://localhost{}", uri)).unwrap();
        let q = url.query_pairs();
        let mut req = HlsRequest::default();
        for (key, value) in q {
            match &*key {
                "_HLS_msn" => {
                    if let Ok(msn) = value.parse() {
                        req.msn = Some(msn);
                    }
                },
                "_HLS_part" => {
                    if let Ok(part) = value.parse() {
                        req.part = Some(part);
                    }
                },
                "_HLS_push" => {
                    if let Ok(push) = value.parse() {
                        req.push = Some(push);
                    }
                },
                _ => {}
            }
        }
        req
    }

    fn media_manifest(req: Request<Body>, store: &mut store::Store, id: store::TrackId) -> Either<ImmediateFut, MediaManifestFut> {
        let hls_request = Self::hls_request_params(req.uri());
        if let Some(request_msn) = hls_request.msn {
            let current_msn = {
                let mut track_ref = store.get_track(id).unwrap();
                match track_ref.track() {
                    store::Track::Avc(ref avc_track) => avc_track.media_sequence_number(),
                    store::Track::Aac(ref aac_track) => aac_track.media_sequence_number(),
                }
            };
            if request_msn > current_msn + 1 {
                // per the spec, return HTTP 400 error response
                println!("Sequence number {} requested too early ({} + {})", request_msn, current_msn, request_msn - current_msn);
                return Either::A(futures::future::ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(format!("Sequence number {} requested too early ({} + {})", request_msn, current_msn, request_msn - current_msn)))
                    .unwrap()))
            }
            if request_msn == current_msn + 1 {
                // block waiting for the next segment
                return Either::B(Box::new(Self::block_for_media_manifest(store, id, hls_request)));
            }
        }
        let has_pts_to_utc = store.has_pts_to_utc();
        let mut track_ref = store.get_track(id).unwrap();
        let text = Self::render_media_manifest(has_pts_to_utc, track_ref);

        Either::A(futures::future::ok(Response::builder()
            .header("Content-Type", "application/vnd.apple.mpegurl")
            .header("Access-Control-Allow-Origin", "*")
            .body(Body::from(text))
            .unwrap()))
    }

    fn block_for_media_manifest(store: &mut store::Store, id: store::TrackId, req: HlsRequest) -> impl Future<Item=Response<Body>, Error=HlsServiceError> {
        let msn = req.msn.unwrap();
        let seq_stream = {
            let mut track_ref = store.get_track(id).unwrap();
            match track_ref.track() {
                store::Track::Avc(ref avc_track) => avc_track.sequence_stream(),
                store::Track::Aac(ref aac_track) => aac_track.sequence_stream(),
            }
        };
        let mut store = store.clone();
        seq_stream
            .skip_while(move |seq| future::ok(seq.seg < msn) )
            .skip_while(move |seq| future::ok(seq.seg == msn && req.part.map(|p| seq.part < p ).unwrap_or(false)) )
            .into_future()
            .map_err(|(e, _stream)| panic!("Unexpected watch error {:?}", e) )
            .and_then(move |(seq, _stream)| {
                let has_pts_to_utc = store.has_pts_to_utc();
                let mut track_ref = store.get_track(id).unwrap();
                let text = Self::render_media_manifest(has_pts_to_utc, track_ref);
                let mut b = Response::builder();
                b.header("Content-Type", "application/vnd.apple.mpegurl");
                b.header("Access-Control-Allow-Origin", "*");
                if let Some(seq) = seq {
                    if req.push.map(|p| p > 0).unwrap_or(false) {
                        if let Some(part) = req.part {
                            let mut track_ref = store.get_track(id).expect("TODO: get_track()");
                            // jump through some hoops to map from the segment number to its timestamp
                            let segment = match track_ref.track() {
                                store::Track::Avc(ref avc_track) => avc_track.segments().find(|s| s.sequence_number() == seq.seg ),
                                store::Track::Aac(ref aac_track) => aac_track.segments().find(|s| s.sequence_number() == seq.seg ),
                            }.unwrap_or_else(|| panic!("Couldn't get segment #{} of track {:?}", seq.seg, id) );
                            b.header("Link", format!("</track/{}/segment/{}/part/{}.mp4>; rel=preload; as=video; type=video/mp4", id.0, segment.id(), part));
                        }
                        // TODO: push segment if no part was requested?
                    }
                }
                futures::future::ok(b.body(Body::from(text)).unwrap())
            })
    }

    fn render_media_manifest(has_pts_to_utc: bool, track_ref: store::TrackRef) -> String {
        let mut text = String::new();
        writeln!(text, "#EXTM3U").unwrap();
        // TODO: validate correct version vs. used HLS features
        writeln!(text, "#EXT-X-VERSION:{}", 7).unwrap();
        writeln!(text, "#EXT-X-INDEPENDENT-SEGMENTS").unwrap();
        writeln!(text, "#EXT-X-PART-INF:PART-TARGET={:.3}", 0.32).unwrap();
        writeln!(text, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:0.3}", 0.96).unwrap();
        let mut track_ref = track_ref;
        match track_ref.track() {
            store::Track::Avc(ref avc_track) => {
                writeln!(text,
                         "#EXT-X-TARGETDURATION:{}",
                         avc_track.max_chunk_duration())
                    .unwrap();
                writeln!(text,
                         "#EXT-X-MAP:URI=\"init.mp4\"")
                    .unwrap();
                if let Some(first) = avc_track.segments().nth(0) {
                    if first.sequence_number() > 0 {
                        writeln!(text,
                                 "#EXT-X-MEDIA-SEQUENCE:{}",
                                 first.sequence_number())
                            .unwrap();
                    }
                    if has_pts_to_utc {
                        let utc_millis = first.id() * 1_000 / Timestamp::TIMEBASE as i64;
                        let date_time = chrono::Utc.timestamp_millis(utc_millis);

                        writeln!(text, "#EXT-X-PROGRAM-DATE-TIME:{}", date_time.format("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap();
                    }
                }
                for seg in avc_track.segments() {
                    if !seg.is_continuous() {
                        writeln!(text, "#EXT-X-DISCONTINUITY").unwrap();
                    }
                    if avc_track.has_parts(seg.id()) {
                        match avc_track.parts(seg.id()) {
                            Ok(parts) => {
                                Self::part_list(&mut text, &seg, parts)
                            },
                            Err(e) => (),
                        }
                    }
                    if let Some(duration) = seg.duration_seconds() {
                        // only expecting the final, in-progress segment to lack duration
                        writeln!(text, "#EXTINF:{:.3},{}", duration, "").unwrap();
                        writeln!(text, "segment/{}/seg.mp4", seg.id()).unwrap();
                    }
                }
            },
            store::Track::Aac(ref aac_track) => {
                writeln!(text,
                         "#EXT-X-TARGETDURATION:{}",
                         aac_track.max_chunk_duration())
                    .unwrap();
                writeln!(text,
                         "#EXT-X-MAP:URI=\"init.mp4\"")
                    .unwrap();
                if let Some(first) = aac_track.segments().nth(0) {
                    if first.sequence_number() > 0 {
                        writeln!(text,
                                 "#EXT-X-MEDIA-SEQUENCE:{}",
                                 first.sequence_number())
                            .unwrap();
                    }
                    if has_pts_to_utc {
                        let utc_millis = first.id() * 1_000 / Timestamp::TIMEBASE as i64;
                        let date_time = chrono::Utc.timestamp_millis(utc_millis);

                        writeln!(text, "#EXT-X-PROGRAM-DATE-TIME:{}", date_time.format("%Y-%m-%dT%H:%M:%S%.3fZ")).unwrap();
                    }
                }
                for seg in aac_track.segments() {
                    if !seg.is_continuous() {
                        writeln!(text, "#EXT-X-DISCONTINUITY").unwrap();
                    }
                    if aac_track.has_parts(seg.id()) {
                        match aac_track.parts(seg.id()) {
                            Ok(parts) => {
                                Self::part_list(&mut text, &seg, parts)
                            },
                            Err(e) => (),
                        }
                    }
                    if let Some(duration) = seg.duration_seconds() {
                        // only expecting the final, in-progress segment to lack duration
                        writeln!(text, "#EXTINF:{:.3},{}", duration, "").unwrap();
                        writeln!(text, "segment/{}/seg.mp4", seg.id()).unwrap();
                    }
                }
            },
        }
        text
    }

    fn part_list(text: &mut String, seg: &store::SegmentInfo, parts: impl Iterator<Item=store::PartInfo>) -> () {
        for part in parts {
            write!(text,
                   "#EXT-X-PART:DURATION={:.3},URI=\"segment/{}/part/{}.mp4\"",
                   part.duration_seconds().unwrap(),
                   seg.id(),
                   part.id()).unwrap();
            if part.is_independent() {
                write!(text, ",INDEPENDENT=YES").unwrap();
            }
            writeln!(text).unwrap();
        }
    }

    fn initialisation_segment(req: Request<Body>, track_ref: store::TrackRef) -> ImmediateFut {
        let mut track_ref = track_ref;
        let init = match track_ref.track() {
            store::Track::Avc(ref avc_track) => {
                Self::make_avc_initialisation_segment(avc_track)
            },
            store::Track::Aac(ref avc_track) => {
                Self::make_aac_initialisation_segment(avc_track)
            },
        };
        let init = match init {
            Ok(init) => init,
            Err(e) => {
                eprintln!("Problem creating initialisation segment of track {}: {:?}", track_ref.id().0, e);
                return futures::future::ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("Problem creating initialisation segment"))
                    .unwrap())
            }
        };

        let mut data = vec![];
        init.write_to(&mut data).unwrap();

        futures::future::ok(Response::builder()
            .header("Content-Type", "video/mp4")
            .header("Access-Control-Allow-Origin", "*")
            .body(Body::from(data))
            .unwrap())
    }

    fn make_avc_initialisation_segment(avc_track: &store::AvcTrack) -> Result<fmp4::InitializationSegment, mse_fmp4::Error> {
        let mut segment = fmp4::InitializationSegment::default();

        let (width, height) = avc_track.dimensions();
        let mut track = fmp4::TrackBox::new(true);
        track.tkhd_box.width = width << 16;
        track.tkhd_box.height = height << 16;
        track.tkhd_box.duration = 0;
        //track.edts_box.elst_box.media_time = avc_track.segments().next().unwrap().id() as i32;
        track.mdia_box.mdhd_box.timescale = 90000;
        track.mdia_box.mdhd_box.duration = 0;

        let sps = avc_track.sps();
        let mut sps_bytes = vec![];
        sps_bytes.extend_from_slice(avc_track.sps_bytes());
        let mut pps_bytes = vec![];
        pps_bytes.extend_from_slice(avc_track.pps_bytes());

        let avc_sample_entry = fmp4::AvcSampleEntry {
            width: width as u16,
            height: height as u16,
            avcc_box: fmp4::AvcConfigurationBox {
                configuration: mse_fmp4::avc::AvcDecoderConfigurationRecord {
                    profile_idc: sps.profile_idc.into(),
                    constraint_set_flag: 0,
                    level_idc: sps.level_idc,
                    sequence_parameter_set: sps_bytes,
                    picture_parameter_set: pps_bytes,
                },
            },
        };
        track
            .mdia_box
            .minf_box
            .stbl_box
            .stsd_box
            .sample_entries
            .push(fmp4::SampleEntry::Avc(avc_sample_entry));
        segment.moov_box.trak_boxes.push(track);
        segment.moov_box.mvhd_box.timescale = 1;
        segment.moov_box.mvhd_box.duration = 0;
        segment
            .moov_box
            .mvex_box
            .trex_boxes
            .push(fmp4::TrackExtendsBox::new(true));

        Ok(segment)
    }

    fn make_aac_initialisation_segment(aac_track: &store::AacTrack) -> Result<fmp4::InitializationSegment, mse_fmp4::Error> {
        let mut segment = fmp4::InitializationSegment::default();

        let mut track = fmp4::TrackBox::new(false);
        track.tkhd_box.duration = 0;
        track.mdia_box.mdhd_box.timescale = 90000;
        track.mdia_box.mdhd_box.duration = 0;

        let profile = match aac_track.profile() {
            adts_reader::AudioObjectType::AacMain => aac::AacProfile::Main,
            adts_reader::AudioObjectType::AacLC => aac::AacProfile::Lc,
            adts_reader::AudioObjectType::AacSSR => aac::AacProfile::Ssr,
            adts_reader::AudioObjectType::AacLTP => aac::AacProfile::Ltp,
        };
        let frequency = aac::SamplingFrequency::from_index(aac_track.frequency() as u8).unwrap();
        let channel_configuration = aac::ChannelConfiguration::from_u8(aac_track.channel_config() as u8).unwrap();


        let aac_sample_entry = fmp4::AacSampleEntry {
            esds_box: fmp4::Mpeg4EsDescriptorBox {
                profile,
                frequency,
                channel_configuration,
            },
        };
        track
            .mdia_box
            .minf_box
            .stbl_box
            .stsd_box
            .sample_entries
            .push(fmp4::SampleEntry::Aac(aac_sample_entry));
        segment.moov_box.trak_boxes.push(track);
        segment.moov_box.mvhd_box.timescale = 1;
        segment.moov_box.mvhd_box.duration = 0;
        segment
            .moov_box
            .mvex_box
            .trex_boxes
            .push(fmp4::TrackExtendsBox::new(false));

        Ok(segment)
    }

    fn fmp4_segment(req: Request<Body>, track_ref: store::TrackRef, sample_id: String, rest: Option<String>) -> Response<Body> {
        let segment_dts = if let Ok(dts) = sample_id.parse() {
            dts
        } else {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Invalid segment id"))
                .unwrap()
        };

        if let Some(rest) = rest {
            if rest.starts_with("part/") {
                let rest = &rest["part/".len()..];
                if !rest.ends_with(".mp4") {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Invalid part request"))
                        .unwrap()
                }
                let part_id = if let Ok(part_id) = rest[..rest.len()-".mp4".len()].parse() {
                    part_id
                } else {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Invalid part id"))
                        .unwrap()
                };

                let mut track_ref = track_ref;
                let segment = match track_ref.track() {
                    store::Track::Avc(ref avc_track) => {
                        Self::make_avc_part(avc_track, segment_dts, part_id)
                    },
                    store::Track::Aac(ref aac_track) => {
                        Self::make_aac_part(aac_track, segment_dts, part_id)
                    },
                };
                let segment = match segment {
                    Ok(segment) => segment,
                    Err(e) => {
                        eprintln!("Problem creating part {} of segment {} of track {}: {:?}", part_id, segment_dts, track_ref.id().0, e);
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from("Problem creating segment"))
                            .unwrap()

                    }
                };

                let mut data = vec![];
                segment.write_to(&mut data).unwrap();
                //data.extend_from_slice(segment.data());

                Response::builder()
                    .header("Content-Type", "video/mp4")
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(data))
                    .unwrap()
            } else if "seg.mp4" == rest {
                let mut track_ref = track_ref;
                let segment = match track_ref.track() {
                    store::Track::Avc(ref avc_track) => {
                        Self::make_avc_segment(avc_track, segment_dts)
                        //Self::make_avc_segment_ffmpeg(avc_track, segment_dts)
                    },
                    store::Track::Aac(ref aac_track) => {
                        Self::make_aac_segment(aac_track, segment_dts)
                    },
                };

                let segment = match segment {
                    Ok(segment) => segment,
                    Err(e) => {
                        eprintln!("Problem creating segment {} of track {}: {:?}", segment_dts, track_ref.id().0, e);
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from("Problem creating segment"))
                            .unwrap()

                    }
                };

                let mut data = vec![];
                segment.write_to(&mut data).unwrap();
                //data.extend_from_slice(segment.data());

                Response::builder()
                    .header("Content-Type", "video/mp4")
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(data))
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Don't know how to produce such a segment"))
                    .unwrap()
            }
        } else {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Don't know how to produce such a segment"))
                .unwrap()
        }
    }
/*
    fn make_avc_segment_ffmpeg(avc_track: &store::AvcTrack, dts: i64) -> crate::fmp4::Buf {
        let mut builder = crate::fmp4::FragmentBuilder::new();
        for sample in avc_track.segment_samples(dts) {
            builder.add_sample(sample.dts, sample.pts, &sample.data[..]);
        }
        builder.finalize()
    }
*/
    fn make_avc_segment(avc_track: &store::AvcTrack, dts: i64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let (initial_dts, avc_stream) = Self::create_avc_stream(avc_track, dts, 0, std::usize::MAX).unwrap(); // TODO

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = avc_track.segment_number_for(dts) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        // video traf
        let mut traf = fmp4::TrackFragmentBox::new(true);
        traf.tfdt_box.base_media_decode_time = initial_dts as u32;
        traf.tfhd_box.default_sample_flags = Some(fmp4::SampleFlags {
            is_leading: 0,
            sample_depends_on: 1,
            sample_is_depdended_on: 0,
            sample_has_redundancy: 0,
            sample_padding_value: 0,
            sample_is_non_sync_sample: true,
            sample_degradation_priority: 0,
        });
        traf.trun_box.data_offset = Some(0); // dummy
        traf.trun_box.first_sample_flags = Some(fmp4::SampleFlags {
            is_leading: 0,
            sample_depends_on: 2,
            sample_is_depdended_on: 0,
            sample_has_redundancy: 0,
            sample_padding_value: 0,
            sample_is_non_sync_sample: false,
            sample_degradation_priority: 0,
        });
        traf.trun_box.samples = avc_stream.samples;
        segment.moof_box.traf_boxes.push(traf);

        // mdat and offsets adjustment
        let mut counter = mse_fmp4::io::ByteCounter::with_sink();
        segment.moof_box.write_box(&mut counter)?;
        segment.moof_box.traf_boxes[0].trun_box.data_offset = Some(counter.count() as i32 + 8);

        segment.mdat_boxes.push(fmp4::MediaDataBox {
            data: avc_stream.data,
        });

        Ok(segment)
    }

    fn make_avc_part(avc_track: &store::AvcTrack, dts: i64, part_id: u64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let (initial_dts, avc_stream) = Self::create_avc_stream(avc_track, dts, part_id as usize, store::AvcTrack::VIDEO_SAMPLES_PER_PART).unwrap(); // TODO

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = avc_track.part_number_for(dts, part_id) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        // video traf
        let mut traf = fmp4::TrackFragmentBox::new(true);
        traf.tfdt_box.base_media_decode_time = initial_dts as u32;
        traf.tfhd_box.default_sample_flags = Some(fmp4::SampleFlags {
            is_leading: 0,
            sample_depends_on: 1,
            sample_is_depdended_on: 0,
            sample_has_redundancy: 0,
            sample_padding_value: 0,
            sample_is_non_sync_sample: true,
            sample_degradation_priority: 0,
        });
        traf.trun_box.data_offset = Some(0); // dummy
        traf.trun_box.first_sample_flags = Some(fmp4::SampleFlags {
            is_leading: 0,
            sample_depends_on: 2,
            sample_is_depdended_on: 0,
            sample_has_redundancy: 0,
            sample_padding_value: 0,
            sample_is_non_sync_sample: false,
            sample_degradation_priority: 0,
        });
        traf.trun_box.samples = avc_stream.samples;
        segment.moof_box.traf_boxes.push(traf);

        // mdat and offsets adjustment
        let mut counter = mse_fmp4::io::ByteCounter::with_sink();
        segment.moof_box.write_box(&mut counter)?;
        segment.moof_box.traf_boxes[0].trun_box.data_offset = Some(counter.count() as i32 + 8);

        segment.mdat_boxes.push(fmp4::MediaDataBox {
            data: avc_stream.data,
        });

        Ok(segment)
    }

    // reformat the data into the form accepted by the mse_fmp4 crate
    fn create_avc_stream(avc_track: &store::AvcTrack, dts: i64, offset: usize, limit: usize) -> Result<(u64, AvcStream), store::SegmentError> {
        let mut avc_stream = AvcStream {
            samples: vec![],
            data: vec![]
        };
        let mut avc_timestamps = Vec::new();
        let mut avc_timestamp_offset = 0;

        for sample in avc_track.segment_samples(dts)?.skip(offset * store::AvcTrack::VIDEO_SAMPLES_PER_PART).take(limit) {
            let i = avc_timestamps.len();
            let mut timestamp = sample.pts;
            if i == 0 {
                avc_timestamp_offset = timestamp;
            }
            if timestamp < avc_timestamp_offset {
                // TODO: this code for handling TS wrap is from mse_fmp4; maybe an underlying Timestamp type could handle this directly
                timestamp += Timestamp::MAX.value() as i64;
            }
            avc_timestamps.push((timestamp - avc_timestamp_offset, i));

            let prev_data_len = avc_stream.data.len();
            avc_stream
                .data
                .write_u32::<byteorder::BigEndian>(sample.data.len() as u32)
                .unwrap();
            avc_stream.data.write_all(&sample.data[..]).unwrap();

            let sample_size = (avc_stream.data.len() - prev_data_len) as u32;
            let sample_composition_time_offset = (sample.pts as i64 - sample.dts as i64) as i32;
            avc_stream.samples.push(fmp4::Sample {
                duration: None, // dummy
                size: Some(sample_size),
                flags: None,
                composition_time_offset: Some(sample_composition_time_offset),
            });
        }
        avc_timestamps.sort();
        for (&(curr, _), &(next, i)) in avc_timestamps.iter().zip(avc_timestamps.iter().skip(1)) {
            let duration = next - curr;
            avc_stream.samples[i].duration = Some(duration as u32);
        }
        if !avc_stream.samples.is_empty() {
            //avc_stream.samples[0].duration = Some(cmp::max(0, avc_stream.start_time()) as u32);
            // TODO: calculate durations in some better manner!
            avc_stream.samples[0].duration = Some(3600);
        }

        Ok((avc_timestamp_offset as u64, avc_stream))
    }

    fn make_aac_segment(aac_track: &store::AacTrack, dts: i64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let (initial_dts, aac_stream) = Self::create_aac_stream(aac_track, dts, 0, std::usize::MAX).unwrap();

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = aac_track.segment_number_for(dts) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        let mut traf = fmp4::TrackFragmentBox::new(false);
        traf.tfdt_box.base_media_decode_time = initial_dts as u32;
        traf.tfhd_box.default_sample_duration = Some(aac::SAMPLES_IN_FRAME as u32);
        traf.trun_box.data_offset = Some(0); // dummy
        traf.trun_box.samples = aac_stream.samples;
        segment.moof_box.traf_boxes.push(traf);

        // mdat and offsets adjustment
        let mut counter = mse_fmp4::io::ByteCounter::with_sink();
        segment.moof_box.write_box(&mut counter)?;
        segment.moof_box.traf_boxes[0].trun_box.data_offset = Some(counter.count() as i32 + 8);

        segment.mdat_boxes.push(fmp4::MediaDataBox {
            data: aac_stream.data,
        });

        Ok(segment)
    }

    fn make_aac_part(aac_track: &store::AacTrack, dts: i64, part_id: u64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let (initial_dts, aac_stream) = Self::create_aac_stream(aac_track, dts, part_id as usize, store::AacTrack::AUDIO_FRAMES_PER_PART).unwrap(); // TODO

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = aac_track.part_number_for(dts, part_id) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        let mut traf = fmp4::TrackFragmentBox::new(false);
        traf.tfdt_box.base_media_decode_time = initial_dts as u32;
        traf.tfhd_box.default_sample_duration = Some(aac::SAMPLES_IN_FRAME as u32);
        traf.trun_box.data_offset = Some(0); // dummy
        traf.trun_box.samples = aac_stream.samples;
        segment.moof_box.traf_boxes.push(traf);

        // mdat and offsets adjustment
        let mut counter = mse_fmp4::io::ByteCounter::with_sink();
        segment.moof_box.write_box(&mut counter)?;
        segment.moof_box.traf_boxes[0].trun_box.data_offset = Some(counter.count() as i32 + 8);

        segment.mdat_boxes.push(fmp4::MediaDataBox {
            data: aac_stream.data,
        });

        Ok(segment)
    }

    fn create_aac_stream(avc_track: &store::AacTrack, dts: i64, offset: usize, limit: usize) -> Result<(u64, AacStream), SegmentError> {
        let mut aac_stream = AacStream {
            samples: vec![],
            data: vec![]
        };
        let mut aac_timestamps = Vec::new();
        let mut aac_timestamp_offset = 0;

        for sample in avc_track.segment_samples(dts).skip(offset * store::AacTrack::AUDIO_FRAMES_PER_PART).take(limit) {
            let i = aac_timestamps.len();
            let mut timestamp = sample.pts;
            if i == 0 {
                aac_timestamp_offset = timestamp;
            }
            if timestamp < aac_timestamp_offset {
                // TODO: this code for handling TS wrap is from mse_fmp4; maybe an underlying Timestamp type could handle this directly
                timestamp += Timestamp::MAX.value() as i64;
            }
            aac_timestamps.push((timestamp - aac_timestamp_offset, i));

            let prev_data_len = aac_stream.data.len();
            aac_stream.data.write_all(&sample.data[..]).unwrap();

            let sample_size = (aac_stream.data.len() - prev_data_len) as u32;
            let sample_composition_time_offset = (sample.pts as i64 - sample.dts as i64) as i32;
            aac_stream.samples.push(fmp4::Sample {
                // TODO: calculate durations in some better manner!
                duration: Some(1920),
                size: Some(sample_size),
                flags: None,
                composition_time_offset: Some(sample_composition_time_offset),
            });
        }

        Ok((aac_timestamp_offset as u64, aac_stream))
    }
}
impl futures::IntoFuture for HlsService {
    type Future = future::FutureResult<Self::Item, Self::Error>;
    type Item = Self;
    type Error = HlsServiceError;

    fn into_future(self) -> Self::Future {
        future::ok(self)
    }
}

#[derive(Default, Clone, Copy)]
struct HlsRequest {
    msn: Option<u64>,
    part: Option<u16>,
    push: Option<u16>,
}

#[derive(Debug)]
enum HlsServiceError {
    Unimplemented
}
impl error::Error for HlsServiceError {

}
impl Display for HlsServiceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt::Debug::fmt(self, f)
    }
}

pub fn create_server(store: store::Store) -> impl Future<Item=(), Error=()> {
    let addr = ([0, 0, 0, 0], 5050).into();

    // A `Service` is needed for every connection, so this
    // creates one from our `hello_world` function.
    let new_svc = move || {
        HlsService { store: store.clone() }
    };

    Server::bind(&addr)
        .serve(new_svc)
        .map_err(|e| eprintln!("server error: {}", e))
}


#[derive(Debug)]
struct AvcStream {
    samples: Vec<fmp4::Sample>,
    data: Vec<u8>,
}
impl AvcStream {
    fn duration(&self) -> Result<u32, mse_fmp4::Error> {
        let mut duration: u32 = 0;
        for sample in &self.samples {
            let sample_duration = sample.duration.ok_or(mse_fmp4::ErrorKind::InvalidInput)?;
            duration = duration.checked_add(sample_duration).ok_or(mse_fmp4::ErrorKind::InvalidInput)?;
        }
        Ok(duration)
    }
    fn start_time(&self) -> i32 {
        self.samples
            .first()
            .and_then(|s| s.composition_time_offset)
            .unwrap_or(0)
    }
}

#[derive(Debug)]
struct AacStream {
    samples: Vec<fmp4::Sample>,
    data: Vec<u8>,
}
impl AacStream {
    fn duration(&self) -> Result<u32, mse_fmp4::Error> {
        let mut duration: u32 = 0;
        for sample in &self.samples {
            let sample_duration = sample.duration.ok_or(mse_fmp4::ErrorKind::InvalidInput)?;
            duration = duration.checked_add(sample_duration).ok_or(mse_fmp4::ErrorKind::InvalidInput)?;
        }
        Ok(duration)
    }
    fn start_time(&self) -> i32 {
        self.samples
            .first()
            .and_then(|s| s.composition_time_offset)
            .unwrap_or(0)
    }
}
