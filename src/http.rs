use hyper::{Body, Request, Response, Server, StatusCode};
use futures::future::Future;
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

struct HlsService {
    store: store::Store,
}
impl Service for HlsService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = HlsServiceError;
    type Future = future::FutureResult<Response<Self::ResBody>, Self::Error>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let path = req.uri().path();
        let resp = if path == "/stream.html" {
            self.stream_html(req)
        } else if path.starts_with("/master.m3u8") {
            self.master_manifest(req)
        } else if path.starts_with("/track/") {
            let mut parts = path["/track/".len()..].splitn(2, "/");
            let id = parts.next();
            let rest = parts.next();
            if let Some(id) = id {
                let id = id.to_string();
                let rest = rest.map(|s| s.to_string() );
                self.track(req, id, rest)
            } else {
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Need a track id"))
                    .unwrap()
            }
        } else {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("I don't know that one"))
                .unwrap()
        };
        futures::future::ok(resp)
    }
}
impl HlsService {
    fn stream_html(&mut self, req: Request<Body>) -> Response<Body> {
        let mut text = String::new();
        text.write_str("<html><body>\n").unwrap();
        text.write_str("<h1>Stream info</h1>\n").unwrap();
        text.write_str("<h2>Playback</h2>\n").unwrap();
        text.write_str("<p><a href=\"master.m3u8\">master.m3u8</a></p>\n").unwrap();
        text.write_str("<h2>Track list</h2>\n").unwrap();
        if self.store.track_list().count() == 0 {
            text.write_str("<p><em>No tracks!</em></p>\n").unwrap();
        } else {
            text.write_str("<ul>\n").unwrap();
            for track in self.store.track_list() {
                writeln!(text, "<li><a href=\"track/{track_id}/track.html\">Track {track_id}</a></li>", track_id = track.track_id.0).unwrap();
            }
            text.write_str("</ul>\n").unwrap();
        }
        text.write_str("</body></html>\n").unwrap();
        Response::builder()
            .header("Content-Type", "text/html")
            .body(Body::from(text))
            .unwrap()
    }

    fn master_manifest(&mut self, req: Request<Body>) -> Response<Body> {
        let mut text = String::new();
        writeln!(text, "#EXTM3U").unwrap();
        // TODO: validate correct version vs. used HSL features
        writeln!(text, "#EXT-X-VERSION:{}", 9).unwrap();
        writeln!(text, "#EXT-X-INDEPENDENT-SEGMENTS").unwrap();
        writeln!(text, "").unwrap();

        // TODO: audio, keyframes

        for track in self.store.track_list() {
            match self.store.get_track(track.track_id).unwrap().track() {
                store::Track::Avc(avc_track) => {
                    // TODO:
                    //  - FRAMERATE
                    let (width, height) = avc_track.dimensions();
                    writeln!(text,
                             "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},AUDIO=\"default-audio-group\"",
                             avc_track.bandwidth(),
                             //avc_track.rfc6381_codec(),
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
        Response::builder()
            .header("Content-Type", "application/vnd.apple.mpegurl")
            .header("Access-Control-Allow-Origin", "*")
            .body(Body::from(text))
            .unwrap()
    }

    fn track(&mut self, req: Request<Body>, track_id: String, rest: Option<String>) -> Response<Body> {
        if let Ok(id) = track_id.parse() {
            if let Some(track) = self.store.get_track(store::TrackId(id)) {
                if let Some(rest) = rest {
                    if "track.html" == rest {
                        Self::track_html(req, track)
                    } else if "media.m3u8" == rest {
                        Self::media_manifest(req, track)
                    } else if "init.mp4" == rest {
                        Self::initialisation_segment(req, track)
                    } else if rest.starts_with("segment/") {
                        let mut parts = rest["segment/".len()..].splitn(2, "/");
                        let id = parts.next();
                        let rest = parts.next();
                        if let Some(id) = id {
                            let id = id.to_string();
                            let rest = rest.map(|s| s.to_string() );
                            Self::fmp4_segment(req, track, id, rest)
                        } else {
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from("Need a segment id"))
                                .unwrap()
                        }
                    } else if rest.starts_with("sample/") {
                        let mut parts = rest["sample/".len()..].splitn(2, "/");
                        let id = parts.next();
                        let rest = parts.next();
                        if let Some(id) = id {
                            let id = id.to_string();
                            let rest = rest.map(|s| s.to_string() );
                            Self::sample_html(req, track, id, rest)
                        } else {
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from("Need a sample id"))
                                .unwrap()
                        }

                    } else {
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("Don't know how to do that to a track"))
                            .unwrap()
                    }
                } else {
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::from("What do you want to do with this track?"))
                        .unwrap()
                }
            } else {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("No such track"))
                    .unwrap()
            }
        } else {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Bad track id"))
                .unwrap()
        }
    }

    fn media_manifest(req: Request<Body>, track_ref: store::TrackRef) -> Response<Body> {
        let mut text = String::new();
        writeln!(text, "#EXTM3U").unwrap();
        // TODO: validate correct version vs. used HSL features
        writeln!(text, "#EXT-X-VERSION:{}", 9).unwrap();
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
                writeln!(text, "#EXT-X-PROGRAM-DATE-TIME:2019-02-14T02:13:36.106Z").unwrap();
                // TODO: EXT-X-MEDIA-SEQUENCE
                for seg in avc_track.segments() {
                    if !seg.is_continuous() {
                        writeln!(text, "#EXT-X-DISCONTINUITY").unwrap();
                    }
                    match avc_track.parts(seg.id()) {
                        Ok(parts) => {
                            Self::part_list(&mut text, &seg, parts)
                        },
                        Err(e) => (),
                    };
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
                writeln!(text, "#EXT-X-PROGRAM-DATE-TIME:2019-02-14T02:13:36.106Z").unwrap();
                // TODO: EXT-X-MEDIA-SEQUENCE
                for seg in aac_track.segments() {
                    if !seg.is_continuous() {
                        writeln!(text, "#EXT-X-DISCONTINUITY").unwrap();
                    }
                    match aac_track.parts(seg.id()) {
                        Ok(parts) => {
                            Self::part_list(&mut text, &seg, parts)
                        },
                        Err(e) => (),
                    };
                    if let Some(duration) = seg.duration_seconds() {
                        // only expecting the final, in-progress segment to lack duration
                        writeln!(text, "#EXTINF:{:.3},{}", duration, "").unwrap();
                        writeln!(text, "segment/{}/seg.mp4", seg.id()).unwrap();
                    }
                }
            },
        }

        Response::builder()
            .header("Content-Type", "application/vnd.apple.mpegurl")
            .header("Access-Control-Allow-Origin", "*")
            .body(Body::from(text))
            .unwrap()
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

    fn initialisation_segment(req: Request<Body>, track_ref: store::TrackRef) -> Response<Body> {
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
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("Problem creating initialisation segment"))
                    .unwrap()

            }
        };

        let mut data = vec![];
        init.write_to(&mut data).unwrap();

        Response::builder()
            .header("Content-Type", "video/mp4")
            .header("Access-Control-Allow-Origin", "*")
            .body(Body::from(data))
            .unwrap()
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

    fn track_html(req: Request<Body>, track_ref: store::TrackRef) -> Response<Body> {
        let mut text = String::new();
        text.write_str("<html><body>\n").unwrap();
        writeln!(text, "<h1>Track {}</h1>", track_ref.id().0).unwrap();

        let mut track_ref = track_ref;
        match track_ref.track() {
            store::Track::Avc(ref avc_track) => Self::avc_track_html(req, &mut text, avc_track),
            store::Track::Aac(ref aac_track) => Self::aac_track_html(req, &mut text, aac_track),
        }

        text.write_str("</body></html>\n").unwrap();
        Response::builder()
            .header("Content-Type", "text/html")
            .body(Body::from(text))
            .unwrap()
    }

    fn avc_track_html(req: Request<Body>, text: &mut String, track: &store::AvcTrack) {
        writeln!(text, "<h2>Sequence Parameter Set</h2>").unwrap();
        writeln!(text, "<pre>{:#?}</pre>", track.sps()).unwrap();

        writeln!(text, "<h2>Picture Parameter Set</h2>").unwrap();
        writeln!(text, "<pre>{:#?}</pre>", track.pps()).unwrap();

        writeln!(text, "<h2>Samples</h2>").unwrap();
        writeln!(text, "<ul>").unwrap();
        for sample in track.samples() {
            if let store::SampleHeader::Avc(nal_header, _) = sample.header {
                writeln!(text, "<li><a href=\"sample/{dts}/sample.html\">dts={dts}</a> pts={pts} {unit_type:?}</li>", dts = sample.dts, pts = sample.pts, unit_type = nal_header.nal_unit_type()).unwrap();
            } else {
                writeln!(text, "<li><a href=\"sample/{dts}/sample.html\">dts={dts}</a> pts={pts}</li>", dts = sample.dts, pts = sample.pts).unwrap();
            }
        }
        writeln!(text, "</ul>").unwrap();
    }

    fn aac_track_html(req: Request<Body>, text: &mut String, track: &store::AacTrack) {

        writeln!(text, "<h2>Profile</h2>").unwrap();
        writeln!(text, "<pre>{:?}</pre>", track.profile()).unwrap();
        writeln!(text, "<h2>Channel configuration</h2>").unwrap();
        writeln!(text, "<pre>{:?}</pre>", track.channel_config()).unwrap();
        writeln!(text, "<h2>Sampling Frequency</h2>").unwrap();
        writeln!(text, "<pre>{:?}</pre>", track.frequency()).unwrap();

        writeln!(text, "<h2>Samples</h2>").unwrap();
        writeln!(text, "<ul>").unwrap();
        for sample in track.samples() {
            writeln!(text, "<li><a href=\"sample/{dts}/sample.html\">dts={dts}</a> pts={pts}</li>", dts = sample.dts, pts = sample.pts).unwrap();
        }
        writeln!(text, "</ul>").unwrap();
    }

    fn sample_html(req: Request<Body>, track_ref: store::TrackRef, sample_id: String, rest: Option<String>) -> Response<Body> {
        let sample_dts = if let Ok(dts) = sample_id.parse() {
            dts
        } else {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Invalid such sample id"))
                .unwrap()
        };

        let mut text = String::new();
        text.write_str("<html><body>\n").unwrap();
        writeln!(text, "<h1>Track {} sample {}</h1>", track_ref.id().0, sample_dts).unwrap();

        let mut track_ref = track_ref;
        match track_ref.track() {
            store::Track::Avc(ref avc_track) => {
                if let Some(sample) = avc_track.sample(sample_dts) {
                    Self::sample_detail_html(req, &mut text, sample)
                } else {
                    return Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::from("No such sample"))
                        .unwrap()
                }
            },
            store::Track::Aac(ref aac_track) => {
                if let Some(sample) = aac_track.sample(sample_dts) {
                    Self::sample_detail_html(req, &mut text, sample)
                } else {
                    return Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::from("No such sample"))
                        .unwrap()
                }
            },
        }

        text.write_str("</body></html>\n").unwrap();
        Response::builder()
            .header("Content-Type", "text/html")
            .body(Body::from(text))
            .unwrap()
    }

    fn sample_detail_html(req: Request<Body>, text: &mut String, sample: &store::Sample) {
        writeln!(text, "<dl>").unwrap();
        writeln!(text, "<dt>Size</dt><dd>{} bytes</dd>", sample.data.len()).unwrap();
        writeln!(text, "<dt>DTS</dt><dd>{} ticks</dd>", sample.dts).unwrap();
        writeln!(text, "<dt>PTS</dt><dd>{} ticks</dd>", sample.pts).unwrap();
        writeln!(text, "</dl>").unwrap();

        match sample.header {
            store::SampleHeader::Avc(ref nal_header, ref slice_header) => {
                writeln!(text, "<h2>Slice Header</h2>").unwrap();
                writeln!(text, "<pre>{:#?}</pre>", nal_header).unwrap();
                writeln!(text, "<pre>{:#?}</pre>", slice_header).unwrap();
            }
            store::SampleHeader::Aac => {
                // nothing yet
            }
        }
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
                if !rest.starts_with(".mp4") {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Invalid part request"))
                        .unwrap()
                }
                let part_id = if let Ok(part_id) = rest[..".mp4".len()].parse() {
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
    fn make_avc_segment_ffmpeg(avc_track: &store::AvcTrack, dts: u64) -> crate::fmp4::Buf {
        let mut builder = crate::fmp4::FragmentBuilder::new();
        for sample in avc_track.segment_samples(dts) {
            builder.add_sample(sample.dts, sample.pts, &sample.data[..]);
        }
        builder.finalize()
    }
*/
    fn make_avc_segment(avc_track: &store::AvcTrack, dts: u64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let avc_stream = Self::create_avc_stream(avc_track, dts, 0, std::usize::MAX).unwrap(); // TODO

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = avc_track.segment_number_for(dts) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        // video traf
        let mut traf = fmp4::TrackFragmentBox::new(true);
        traf.tfdt_box.base_media_decode_time = dts as u32;
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

    fn make_avc_part(avc_track: &store::AvcTrack, dts: u64, part_id: u64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let avc_stream = Self::create_avc_stream(avc_track, dts, part_id as usize, store::AvcTrack::VIDEO_SAMPLES_PER_PART).unwrap(); // TODO

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = avc_track.segment_number_for(dts) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        // video traf
        let mut traf = fmp4::TrackFragmentBox::new(true);
        traf.tfdt_box.base_media_decode_time = dts as u32;
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
    fn create_avc_stream(avc_track: &store::AvcTrack, dts: u64, offset: usize, limit: usize) -> Result<AvcStream, store::SegmentError> {
        let mut avc_stream = AvcStream {
            samples: vec![],
            data: vec![]
        };
        let mut avc_timestamps = Vec::new();
        let mut avc_timestamp_offset = 0;

        for sample in avc_track.segment_samples(dts)?.skip(offset).take(limit) {
            let i = avc_timestamps.len();
            let mut timestamp = sample.pts;
            if i == 0 {
                avc_timestamp_offset = timestamp;
            }
            if timestamp < avc_timestamp_offset {
                // TODO: this code for handling TS wrap is from mse_fmp4; maybe an underlying Timestamp type could handle this directly
                timestamp += Timestamp::MAX.value();
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

        Ok(avc_stream)
    }

    fn make_aac_segment(aac_track: &store::AacTrack, dts: u64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        let aac_stream = Self::create_aac_stream(aac_track, dts);

        let mut segment = fmp4::MediaSegment::default();
        if let Some(seq) = aac_track.segment_number_for(dts) {
            segment.moof_box.mfhd_box.sequence_number = seq as u32;
        }

        let mut traf = fmp4::TrackFragmentBox::new(false);
        traf.tfdt_box.base_media_decode_time = dts as u32;
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
    fn make_aac_part(avc_track: &store::AacTrack, dts: u64, part_id: u64) -> Result<fmp4::MediaSegment, mse_fmp4::Error> {
        unimplemented!()
    }

    fn create_aac_stream(avc_track: &store::AacTrack, dts: u64) -> AacStream {
        let mut aac_stream = AacStream {
            samples: vec![],
            data: vec![]
        };
        let mut aac_timestamps = Vec::new();
        let mut aac_timestamp_offset = 0;

        for sample in avc_track.segment_samples(dts) {
            let i = aac_timestamps.len();
            let mut timestamp = sample.pts;
            if i == 0 {
                aac_timestamp_offset = timestamp;
            }
            if timestamp < aac_timestamp_offset {
                // TODO: this code for handling TS wrap is from mse_fmp4; maybe an underlying Timestamp type could handle this directly
                timestamp += Timestamp::MAX.value();
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

        aac_stream
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

#[derive(Debug)]
enum HlsServiceError {

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
