use mpeg2ts_reader::{
    StreamType,
    demultiplex,
    packet,
    pes,
};
use crate::store;
use mpeg2ts_reader::pes::Timestamp;

mod h264;
mod adts;

mpeg2ts_reader::packet_filter_switch! {
    IngestFilterSwitch<IngestDemuxContext> {
        Pat: demultiplex::PatPacketFilter<IngestDemuxContext>,
        Pmt: demultiplex::PmtPacketFilter<IngestDemuxContext>,
        Null: demultiplex::NullPacketFilter<IngestDemuxContext>,
        H264: pes::PesPacketFilter<IngestDemuxContext, h264::H264ElementaryStreamConsumer>,
        Adts: pes::PesPacketFilter<IngestDemuxContext, adts::AdtsElementaryStreamConsumer>,
    }
}
pub struct IngestDemuxContext {
    changeset: demultiplex::FilterChangeset<IngestFilterSwitch>,
    store: store::Store,
}
impl IngestDemuxContext {
    pub fn new(store: store::Store) -> IngestDemuxContext {
        IngestDemuxContext {
            store,
            changeset: Default::default(),
        }
    }
    fn construct_pmt(&self, pid: packet::Pid, program_number: u16) -> demultiplex::PmtPacketFilter<IngestDemuxContext> {
        demultiplex::PmtPacketFilter::new(
            pid,
            program_number,
        )
    }
}
impl demultiplex::DemuxContext for IngestDemuxContext {
    type F = IngestFilterSwitch;

    fn filter_changeset(&mut self) -> &mut demultiplex::FilterChangeset<Self::F> {
        &mut self.changeset
    }
    fn construct(&mut self, req: demultiplex::FilterRequest<'_, '_>) -> Self::F {
        match req {
            demultiplex::FilterRequest::ByPid(packet::Pid::PAT) => {
                IngestFilterSwitch::Pat(demultiplex::PatPacketFilter::default())
            }
            demultiplex::FilterRequest::Pmt {
                pid,
                program_number,
            } => IngestFilterSwitch::Pmt(self.construct_pmt(pid, program_number)),

            demultiplex::FilterRequest::ByStream {
                program_pid, stream_type: StreamType::H264, pmt, stream_info,
            } => IngestFilterSwitch::H264(h264::H264ElementaryStreamConsumer::construct(stream_info, self.store.clone())),

            demultiplex::FilterRequest::ByStream {
                program_pid, stream_type: StreamType::Adts, pmt, stream_info,
            } => IngestFilterSwitch::Adts(adts::AdtsElementaryStreamConsumer::construct(stream_info, self.store.clone())),

            demultiplex::FilterRequest::ByStream { .. } => {
                eprintln!("Ignoring {:?}", req);
                // ignore any other elementary stream-types not handled above,
                IngestFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }

            demultiplex::FilterRequest::ByPid(_) => {
                IngestFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::Nit { .. } => {
                IngestFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
        }
    }
}

pub fn create_demux(store: store::Store) -> (IngestDemuxContext, demultiplex::Demultiplex<IngestDemuxContext>) {
    let mut ctx = IngestDemuxContext::new(store);
    let demux = demultiplex::Demultiplex::new(&mut ctx);
    (ctx, demux)
}

struct UnwrapTimestamp {
    last: Option<Timestamp>,
    carry: u64,
}
impl Default for UnwrapTimestamp {
    fn default() -> Self {
        UnwrapTimestamp {
            last: None,
            carry: 0
        }
    }
}
impl UnwrapTimestamp {
    /// Panics if the `update()` method as never been called
    fn unwrap(&self, ts: Timestamp) -> i64 {
        // check invariant,
        assert_eq!(self.carry & Timestamp::MAX.value(), 0);

        let last = self.last.expect("No previous call to update");
        let diff = ts.value() as i64 - last.value() as i64;
        let half = (Timestamp::MAX.value() / 2) as i64;
        if diff > half {
            ts.value() as i64 + self.carry as i64 - (Timestamp::MAX.value() + 1) as i64
        } else if diff < -(half as i64) {
            ts.value() as i64 + self.carry as i64 + (Timestamp::MAX.value() + 1) as i64
        } else {
            ts.value() as i64 + self.carry as i64
        }
    }

    fn update(&mut self, ts: Timestamp) {
        if let Some (last) = self.last {
            let half = (Timestamp::MAX.value() / 2) as i64;
            let diff = ts.value() as i64 - last.value() as i64;
            if diff < -half {
                self.carry += (Timestamp::MAX.value() + 1);
            }
        }
        self.last = Some(ts);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use mpeg2ts_reader::pes::Timestamp;

    #[test]
    fn basic() {
        let mut unwrap = UnwrapTimestamp::default();
        let a = Timestamp::from_u64(0);
        let b = Timestamp::from_u64(1);

        unwrap.update(a);
        let c = unwrap.unwrap(b);
        assert_eq!(c, 1);
    }

    #[test]
    fn basic_wrap() {
        let mut unwrap = UnwrapTimestamp::default();
        let a = Timestamp::MAX;
        let b = Timestamp::from_u64(0);

        unwrap.update(a);
        let c = unwrap.unwrap(b);
        assert_eq!(c, (Timestamp::MAX.value() + 1) as i64);
    }

    #[test]
    fn backwards() {
        let mut unwrap = UnwrapTimestamp::default();
        let a = Timestamp::from_u64(0);
        let b = Timestamp::MAX;

        unwrap.update(a);
        let c = unwrap.unwrap(b);
        assert_eq!(c, -1 as i64);
    }

}