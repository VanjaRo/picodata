use smol_str::SmolStr;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CopyFlushThresholds {
    rows: usize,
    bytes: Option<usize>,
}

impl CopyFlushThresholds {
    pub(crate) fn new(rows: usize, bytes: Option<usize>) -> Self {
        Self { rows, bytes }
    }

    fn would_exceed(
        self,
        rows: usize,
        bytes: usize,
        next_row_bytes: usize,
    ) -> Option<CopyFlushReasonKind> {
        if rows == 0 {
            return None;
        }
        if rows.saturating_add(1) > self.rows {
            return Some(CopyFlushReasonKind::Rows);
        }
        if self
            .bytes
            .is_some_and(|max_bytes| bytes.saturating_add(next_row_bytes) > max_bytes)
        {
            return Some(CopyFlushReasonKind::Bytes);
        }
        None
    }

    fn reached(self, rows: usize, bytes: usize) -> Option<CopyFlushReasonKind> {
        if rows >= self.rows {
            return Some(CopyFlushReasonKind::Rows);
        }
        if self.bytes.is_some_and(|max_bytes| bytes >= max_bytes) {
            return Some(CopyFlushReasonKind::Bytes);
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyFlushReasonKind {
    Rows,
    Bytes,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CopyFlushThresholdsByScope {
    pub(crate) session: CopyFlushThresholds,
    pub(crate) destination: CopyFlushThresholds,
}

impl CopyFlushThresholdsByScope {
    pub(crate) fn new(session: CopyFlushThresholds, destination: CopyFlushThresholds) -> Self {
        Self {
            session,
            destination,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum CopyDestination {
    Local,
    Replicaset(SmolStr),
}

#[derive(Debug)]
pub(crate) struct PendingCopyRow {
    destination: CopyDestination,
    encoded_row: Vec<u8>,
}

impl PendingCopyRow {
    pub(super) fn new(destination: CopyDestination, encoded_row: Vec<u8>) -> Self {
        Self {
            destination,
            encoded_row,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.encoded_row.len()
    }

    pub(crate) fn destination(&self) -> &CopyDestination {
        &self.destination
    }
}

#[derive(Debug, Default)]
pub(crate) struct PendingCopyRows {
    destinations: HashMap<CopyDestination, DestinationBatch>,
    rows: usize,
    bytes: usize,
}

impl PendingCopyRows {
    pub(crate) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub(crate) fn destination_would_exceed(
        &self,
        row: &PendingCopyRow,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        let Some(batch) = self.destinations.get(&row.destination) else {
            return None;
        };
        limits.would_exceed(batch.encoded_rows.len(), batch.bytes, row.len())
    }

    pub(crate) fn session_would_exceed(
        &self,
        row: &PendingCopyRow,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        limits.would_exceed(self.rows, self.bytes, row.len())
    }

    pub(crate) fn push(&mut self, row: PendingCopyRow) -> CopyDestination {
        let destination = row.destination.clone();
        let row_bytes = row.encoded_row.len();
        self.destinations
            .entry(destination.clone())
            .or_default()
            .push(row.encoded_row);
        self.rows = self.rows.saturating_add(1);
        self.bytes = self.bytes.saturating_add(row_bytes);
        destination
    }

    pub(crate) fn destination_reached(
        &self,
        destination: &CopyDestination,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        let batch = self.destinations.get(destination)?;
        limits.reached(batch.encoded_rows.len(), batch.bytes)
    }

    pub(crate) fn session_reached(
        &self,
        limits: CopyFlushThresholds,
    ) -> Option<CopyFlushReasonKind> {
        limits.reached(self.rows, self.bytes)
    }

    pub(super) fn take_destination(
        &mut self,
        destination: &CopyDestination,
    ) -> Option<DestinationBatch> {
        let batch = self.destinations.remove(destination)?;
        self.rows = self.rows.saturating_sub(batch.encoded_rows.len());
        self.bytes = self.bytes.saturating_sub(batch.bytes);
        Some(batch)
    }

    pub(super) fn destination(&self, destination: &CopyDestination) -> Option<&DestinationBatch> {
        self.destinations.get(destination)
    }

    pub(super) fn contains_destination(&self, destination: &CopyDestination) -> bool {
        self.destinations.contains_key(destination)
    }

    pub(super) fn destinations(&self) -> &HashMap<CopyDestination, DestinationBatch> {
        &self.destinations
    }

    pub(super) fn clear(&mut self) {
        self.rows = 0;
        self.bytes = 0;
        self.destinations.clear();
    }
}

#[derive(Debug, Default)]
pub(super) struct DestinationBatch {
    pub(super) encoded_rows: Vec<Vec<u8>>,
    bytes: usize,
}

impl DestinationBatch {
    pub(super) fn is_empty(&self) -> bool {
        self.encoded_rows.is_empty()
    }

    fn push(&mut self, row: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(row.len());
        self.encoded_rows.push(row);
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_row(destination: CopyDestination, encoded_row: &[u8]) -> PendingCopyRow {
        PendingCopyRow::new(destination, encoded_row.to_vec())
    }

    #[test]
    fn pending_rows_track_destination_and_session_limits() {
        let mut pending = PendingCopyRows::default();
        let limits = CopyFlushThresholds::new(2, Some(4));

        let first = pending_row(CopyDestination::Local, &[1, 2]);
        assert_eq!(pending.destination_would_exceed(&first, limits), None);
        assert_eq!(pending.session_would_exceed(&first, limits), None);

        let destination = pending.push(first);
        assert_eq!(destination, CopyDestination::Local);
        assert_eq!(pending.rows, 1);
        assert_eq!(pending.bytes, 2);
        assert_eq!(pending.destination_reached(&destination, limits), None);
        assert_eq!(pending.session_reached(limits), None);

        let second = pending_row(CopyDestination::Local, &[3, 4, 5]);
        assert_eq!(
            pending.destination_would_exceed(&second, limits),
            Some(CopyFlushReasonKind::Bytes)
        );
        assert_eq!(
            pending.session_would_exceed(&second, limits),
            Some(CopyFlushReasonKind::Bytes)
        );
    }

    #[test]
    fn pending_rows_can_omit_byte_threshold() {
        let mut pending = PendingCopyRows::default();
        let limits = CopyFlushThresholds::new(3, None);

        pending.push(pending_row(CopyDestination::Local, &[1, 2, 3]));

        let next = pending_row(CopyDestination::Local, &[4, 5, 6]);
        assert_eq!(pending.destination_would_exceed(&next, limits), None);
        assert_eq!(pending.session_would_exceed(&next, limits), None);
    }

    #[test]
    fn pending_rows_can_flush_one_destination_without_dropping_others() {
        let mut pending = PendingCopyRows::default();
        let local = CopyDestination::Local;
        let remote = CopyDestination::Replicaset("remote-rs-1".into());

        pending.push(pending_row(local.clone(), &[1, 2]));
        pending.push(pending_row(remote.clone(), &[3, 4, 5]));

        let batch = pending
            .take_destination(&remote)
            .expect("remote destination should be present");
        assert_eq!(batch.encoded_rows, vec![vec![3, 4, 5]]);
        assert_eq!(batch.bytes(), 3);
        assert_eq!(pending.rows, 1);
        assert_eq!(pending.bytes, 2);
        assert!(pending.contains_destination(&local));
        assert!(!pending.contains_destination(&remote));
    }
}
