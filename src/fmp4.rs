use ffmpeg_sys::*;
use std::{ffi, ptr};

const FF_MOV_FLAG_FRAGMENT: i32 = 1 << 1;

pub struct FragmentBuilder {
    context: *mut AVFormatContext,
    stream_index: i32,
}
impl FragmentBuilder {
    pub fn new() -> Self {
        unsafe {
            av_register_all();
            av_log_set_level(AV_LOG_DEBUG);
            let fmt_name = ffi::CString::new("mov").unwrap();
            let fmt = av_guess_format(fmt_name.as_ptr(), ptr::null(), ptr::null());
            if fmt.is_null() {
                panic!("could not find format");
            }
            let mut context = ptr::null_mut();
            let ret = avformat_alloc_output_context2(&mut context, fmt, fmt_name.as_ptr(), ptr::null());
            if ret != 0 {
                panic!("avformat_alloc_output_context2() failed");
            }

            (*(*context).oformat).flags |= FF_MOV_FLAG_FRAGMENT;
            let stream_index;
            if let Some(out_stream) = avformat_new_stream(context, ptr::null()).as_mut() {
                out_stream.time_base.num = 1;
                out_stream.time_base.den = 90000;
                (*out_stream.codecpar).codec_type = AVMediaType::AVMEDIA_TYPE_VIDEO;
                (*out_stream.codecpar).codec_id = AVCodecID::AV_CODEC_ID_H264;
                (*out_stream.codecpar).width = 1024;
                (*out_stream.codecpar).height = 768;

                let codec = ptr::null_mut();
                avcodec_get_context_defaults3(out_stream.codec, codec);
                stream_index = out_stream.index;
            } else {
                panic!("could not create output stream");
            }


            let ret = avio_open_dyn_buf(&mut (*context).pb);
            if ret < 0 {
                panic!("avio_open_dyn_buf() failed: {}", ret);
            }


            let mut options = ptr::null_mut();
            let opt_name = ffi::CString::new("fflags").unwrap();
            let opt_value = ffi::CString::new("-autobsf").unwrap();
            av_dict_set(&mut options, opt_name.as_ptr(), opt_value.as_ptr(), 0);
            let opt_name = ffi::CString::new("movflags").unwrap();
            let opt_value = ffi::CString::new("+frag_custom+dash+delay_moov").unwrap();
            av_dict_set(&mut options, opt_name.as_ptr(), opt_value.as_ptr(), 0);

            let ret = avformat_write_header(context, &mut options);
            if ret < 0 {
                panic!("avformat_write_header() failed: {}", ret);
            }

            FragmentBuilder {
                context,
                stream_index,
            }
        }
    }

    pub fn add_sample(&mut self, dts: u64, pts: u64, data: &[u8]) {
        use std::io::Write;
        unsafe {
            let mut pk = std::mem::uninitialized();
            av_init_packet(&mut pk);
            av_new_packet(&mut pk, data.len() as i32);
            let mut dest = std::slice::from_raw_parts_mut(pk.data, pk.size as usize);
            dest.write(data).unwrap();
            pk.stream_index = self.stream_index;
            pk.dts = dts as i64;
            pk.pts = pts as i64;
            av_interleaved_write_frame(self.context, &mut pk);
            av_free_packet(&mut pk);
        }
    }

    pub fn finalize(self) -> Buf {
        let mut data = ptr::null_mut();
        let size;
        unsafe {
            av_write_trailer(self.context);
            size = avio_close_dyn_buf((*self.context).pb, &mut data);
        }
        if size < 0 {
            panic!("avio_close_dyn_buf() failed {}", size);
        }
        Buf { data, size: size as usize }
    }
}
impl Drop for FragmentBuilder {
    fn drop(&mut self) {
        unsafe { avformat_free_context(self.context) };
    }
}

pub struct Buf {
    data: *mut u8,
    size: usize,
}
impl Buf {
    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.size) }
    }
}
impl Drop for Buf {
    fn drop(&mut self) {
        unsafe {
            av_free(self.data as *mut ffi::c_void);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_works() {
        let build = FragmentBuilder::new();
        let buf = build.finalize();
        hexdump::hexdump(buf.data());
    }
}