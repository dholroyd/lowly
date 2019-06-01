use mpeg2ts_reader::{
    StreamType,
    demultiplex,
    packet,
    psi,
    pes,
};
use crate::store;

mod h264;
mod adts;

mpeg2ts_reader::packet_filter_switch! {
    IngestFilterSwitch<IngestDemuxContext> {
        Pat: demultiplex::PatPacketFilter<IngestDemuxContext>,
        Pmt: demultiplex::PmtPacketFilter<IngestDemuxContext, demultiplex::DemuxPmtProcessor<IngestDemuxContext>>,
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
    fn construct_pmt(&self, pid: packet::Pid, program_number: u16) -> demultiplex::PmtPacketFilter<IngestDemuxContext, demultiplex::DemuxPmtProcessor<IngestDemuxContext>> {
        demultiplex::PmtPacketFilter::new(
            pid,
            program_number,
            demultiplex::DemuxPmtProcessor::new(
                pid,
                program_number
            )
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
