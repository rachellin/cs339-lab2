use std::sync::{Arc, RwLock};

use crate::page::INVALID_PAGE_ID;
use crate::record_id::RecordId;
use crate::{
    buffer_pool::BufferPoolManager, page::table_page::TablePageRef, typedef::PageId, Result,
};
use rustdb_catalog::tuple::Tuple;
use rustdb_error::Error;

use crate::heap::table_heap::TableHeap;

/// An iterator over all non-deleted tuples in a table heap.
///
/// This iterator acquires a read lock on the TableHeap (via an Arc<RwLock<TableHeap>>)
/// and holds the read guard for its lifetime, ensuring that the table remains stable
/// (i.e. unmodified) during iteration.
pub struct TableTupleIterator {
    bpm: Arc<RwLock<BufferPoolManager>>,
    current_page_id: PageId,
    current_slot: u32,
}

impl TableTupleIterator {
    /// Creates a new `TableTupleIterator` by taking an Arc to the table heap's RwLock.
    /// It acquires the read guard internally.
    pub fn new(bpm: Arc<RwLock<BufferPoolManager>>, table_heap: Arc<RwLock<TableHeap>>) -> Self {
        let first_page_id = table_heap.read().unwrap().first_page_id();
        Self {
            bpm,
            current_page_id: first_page_id,
            current_slot: 0,
        }
    }
}

impl Iterator for TableTupleIterator {
    type Item = Result<(rustdb_catalog::schema::RecordId, Tuple)>;

    /// Emits the next non-deleted tuple in the table heap that hasn't yet been emitted by this
    /// iterator, if one exists. Otherwise, if the iterator has exhausted its scan through the
    /// table, returns `None`.
    ///
    /// Note the type of [`Self::Item`] is `Result<(RecordId, Tuple)>`, so any recoverable error
    /// `e: Error<T>` can be propagated to the return value of this method via `Some(Err(e))`.
    /// (The exception to this is an out-of-bounds error, which might signal that the current page
    /// doesn't have more tuples to emit and that the iterator should move to the next page.)
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            //     // stop iterating when we reach the end of the table
            //     if self.current_page_id == INVALID_PAGE_ID {
            //         return None;
            //     }

            //     // get the current page from the buffer pool
            //     let page_handle =
            //         match BufferPoolManager::fetch_page_handle(&self.bpm, self.current_page_id) {
            //             Ok(handle) => handle,          // successfully fetched the page
            //             Err(e) => return Some(Err(e)), // error
            //         };

            //     // create a table page from the page handle
            //     let table_page = TablePageRef::from(page_handle);

            //     // get the next tuple offset
            //     match table_page.get_next_tuple_offset(self.current_slot) {
            //         Ok(Some(rid)) => {
            //             // there is a tuple at this offset

            //             // Try to fetch the tuple at this RecordId
            //             match table_page.get_tuple(&rid) {
            //                 Ok((metadata, tuple)) => {
            //                     // advance to the next slot for the next iteration
            //                     self.current_slot = rid.slot_id() + 1;

            //                     // return tuples that are not deleted
            //                     if !metadata.is_deleted() {
            //                         //return Some(Ok((rid, tuple)));
            //                         return Some(Ok((rid.into(), tuple))); // return the tuple
            //                     } else {
            //                         continue; // skip deleted tuples
            //                     }
            //                 }
            //                 Err(e) => return Some(Err(e)),
            //             }
            //         }
            //         Ok(None) => {
            //             // no more tuples in this page

            //             // move to the next page
            //             match table_page.next_page_id() {
            //                 Some(next_page_id) => {
            //                     self.current_page_id = next_page_id;
            //                     self.current_slot = 0;
            //                 }
            //                 None => {
            //                     // reached end of table
            //                     self.current_page_id = INVALID_PAGE_ID;
            //                 }
            //             }
            //         }
            //         Err(e) => return Some(Err(e)),
            //     }
            // }
            // stop iterating when we reach the end of the table
            if self.current_page_id == INVALID_PAGE_ID {
                return None;
            }

            // get the current page from the buffer pool
            let page_handle =
                match BufferPoolManager::fetch_page_handle(&self.bpm, self.current_page_id) {
                    Ok(handle) => handle,
                    Err(e) => return Some(Err(e)),
                };

            // create a table page from the page handle
            let table_page = TablePageRef::from(page_handle);

            // try to fetch tuple at the current slot
            let rid = RecordId::new(self.current_page_id, self.current_slot);

            match table_page.get_tuple(&rid) {
                Ok((metadata, tuple)) => {
                    self.current_slot += 1; // move to next slot

                    if !metadata.is_deleted() {
                        return Some(Ok((rid.into(), tuple)));
                    }
                    // if deleted, continue to next slot
                    continue;
                }
                Err(Error::OutOfBounds) => {
                    // No tuple at this slot → likely end of page.
                    // Move to the next page.
                    match table_page.next_page_id() {
                        Some(next_page_id) => {
                            self.current_page_id = next_page_id;
                            self.current_slot = 0;
                            continue;
                        }
                        None => {
                            // reached end of table
                            self.current_page_id = INVALID_PAGE_ID;
                            return None;
                        }
                    }
                }
                Err(e) => {
                    // any other error should be propagated
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, RwLock};

    use rustdb_catalog::tuple::Tuple;

    use crate::{
        buffer_pool::BufferPoolManager, disk::disk_manager::DiskManager,
        heap::table_heap::TableHeap, replacer::lru_k_replacer::LrukReplacer, Result,
    };

    use super::TableTupleIterator;

    
    /// Test that the iterator correctly visits all non-deleted tuples in the table heap.
    #[test]
    fn test_table_iterator() -> Result<()> {
        // Set up a test disk and buffer pool manager.
        let disk = Arc::new(Mutex::new(DiskManager::new("test.db").unwrap()));
        let replacer = Box::new(LrukReplacer::new(3));
        let bpm = Arc::new(RwLock::new(BufferPoolManager::new(10, disk, replacer)));

        let mut table_heap = TableHeap::new("table", bpm.clone());

        let tuple1 = Tuple::new(vec![1, 2, 3].into());
        let tuple2 = Tuple::new(vec![4, 5, 6].into());
        let tuple3 = Tuple::new(vec![7, 8, 9].into());
        let tuple4 = Tuple::new(vec![10, 11, 12].into());
        let tuple5 = Tuple::new(vec![13, 14, 15].into());

        table_heap.insert_tuple(&tuple1)?;
        table_heap.insert_tuple(&tuple2)?;
        let rid3 = table_heap.insert_tuple(&tuple3)?;
        table_heap.insert_tuple(&tuple4)?;
        table_heap.insert_tuple(&tuple5)?;

        table_heap.delete_tuple(&rid3).unwrap();

        let iter = TableTupleIterator::new(bpm.clone(), Arc::new(RwLock::new(table_heap)));

        // Collect all tuples from the iterator.
        let tuples: Vec<_> =
            iter.collect::<Result<Vec<(rustdb_catalog::schema::RecordId, Tuple)>>>()?;
        assert_eq!(tuples.len(), 4);
        assert_eq!(tuples[0].1.data().to_vec(), &[1, 2, 3]);
        assert_eq!(tuples[1].1.data().to_vec(), &[4, 5, 6]);
        assert_eq!(tuples[2].1.data().to_vec(), &[10, 11, 12]);
        assert_eq!(tuples[3].1.data().to_vec(), &[13, 14, 15]);

        Ok(())
    }
}
