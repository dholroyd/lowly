# lowly

Low-latency live streaming proof-of-concept.  Attempts to implement the server portion of
[Apple's low-latency extensions to HLS](https://developer.apple.com/documentation/http_live_streaming/protocol_extension_for_low-latency_hls_preliminary_specification).

## Deployment overview

Input:
 - Transport Stream
 - over RTP
 - on UDP port 5000

Output:
 - HTTP1.1 (add HTTP2 support with a proxy like Nginx)
 - on TCP port 5050
 - Master manifest published at `/master.m3u8`

 
```
[encoder]  --MPEGTS/RTP-->  [lowly]  --HLS/HTTP1.1-->  [nginx]  --HLS/HTTP2-->  [player]
```

## ⛔ Limitations
 - Unbounded playlist length!  Old segments are never removed from media-manifests
 - In-memory storage!  The stream will run until the host system runs out of RAM, at which point the process will abort
 - Audio must be AAC 48khz
 - Video must be AVC 25fps, with an IDR every 48 frames
 - Haven't been able to test the low latency aspect on an actual player!  (Standard latency stream has had basic tests
   on Safari / iOS.)
 - prompeg FEC support on input planned but not available yet

## Feature support

Low latency extensions,
 - [x] Partial segments (`EXT-X-PART`)
 - [x] Blocking media-manifest reloads (`_HLS_msn` / `_HLS_part` support)
 - [x] HTTP2 push of parts (if using an HTTP2 reverse proxy supporting `Link: .. rel=preload` header, like Nginx)
 - [ ] Playlist Delta Updates (`EXT-X-SKIP` / `_HLS_skip=YES` not yet supported)
 - [ ] Rendition reports (`EXT-X-RENDITION-REPORT` / `_HLS_report` not yet supported)

General HLS features,
 - [ ] No ABR! (can only ingest a single audio stream and a single video stream right now)
 - Codecs
   - [x] AVC
   - [x] AAC
   - [ ] no others supported (no HEVC etc)
 - [x] fMP4 segments
 - [ ] TS segments unsupported
 - [x] `BANDWIDTH` signalling (supported via `maximum_bitrate_descriptor` in input)
 - [ ] No `FRAMERATE` (planned via inspection of HEVC headers)
 - [ ] No captions / subtitles
 - [ ] No SCTE signalling
 - [ ] No language code signalling (planned via `language_descriptor` metadata in input)
 - [ ] No `EXT-X-DISCONTINUITY` signalling (if the input has a discontinuity, the output will be invalid HLS)
 - [ ] No `EXT-X-PROGRAM-DATE-TIME` (planned via AVC `pic_timing` metadata)
 - [ ] No `EXT-X-I-FRAME-STREAM-INF` / `EXT-X-I-FRAMES-ONLY`
 - [ ] No DRM
 - [ ] No `EXT-X-ENDLIST` (there's currently no way to end the stream)
 - [ ] No `EXT-X-MEDIA-SEQUENCE` since the rewind window never shrinks, msn is never announced (implicitly `1` at start of
       media-manifest)
 - [ ] doubtless lots of other mandatory spec features that are not implemented right now!
 
 
 
 ## See also
 https://en.wikipedia.org/wiki/Lowly_Worm