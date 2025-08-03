//! the code in this file is hoisted from datafusion with only slight modifications
//!
use std::pin::Pin;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use deltalake::kernel::Add;
use deltalake::{DeltaResult, DeltaTableError};
use futures::stream::BoxStream;
use futures::{Future, Stream, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;

/// Trait for types that stream [RecordBatch]
///
/// See [`SendableRecordBatchStream`] for more details.
pub trait RecordBatchStream: Stream<Item = DeltaResult<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a [`Stream`] of [`RecordBatch`]es that can be passed between threads
///
/// This trait is used to retrieve the results of DataFusion execution plan nodes.
///
/// The trait is a specialized Rust Async [`Stream`] that also knows the schema
/// of the data it will return (even if the stream has no data). Every
/// `RecordBatch` returned by the stream should have the same schema as returned
/// by [`schema`](`RecordBatchStream::schema`).
///
/// # See Also
///
/// * [`RecordBatchStreamAdapter`] to convert an existing [`Stream`]
///   to [`SendableRecordBatchStream`]
///
/// [`RecordBatchStreamAdapter`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/stream/struct.RecordBatchStreamAdapter.html
///
/// # Error Handling
///
/// Once a stream returns an error, it should not be polled again (the caller
/// should stop calling `next`) and handle the error.
///
/// However, returning `Ready(None)` (end of stream) is likely the safest
/// behavior after an error. Like [`Stream`]s, `RecordBatchStream`s should not
/// be polled after end of stream or returning an error. However, also like
/// [`Stream`]s there is no mechanism to prevent callers polling  so returning
/// `Ready(None)` is recommended.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub type SendableRBStream = Pin<Box<dyn Stream<Item = DeltaResult<RecordBatch>> + Send>>;

pub type SendableAddStream = Pin<Box<dyn Stream<Item = DeltaResult<Add>> + Send>>;

/// Creates a stream from a collection of producing tasks, routing panics to the stream.
///
/// Note that this is similar to  [`ReceiverStream` from tokio-stream], with the differences being:
///
/// 1. Methods to bound and "detach"  tasks (`spawn()` and `spawn_blocking()`).
///
/// 2. Propagates panics, whereas the `tokio` version doesn't propagate panics to the receiver.
///
/// 3. Automatically cancels any outstanding tasks when the receiver stream is dropped.
///
/// [`ReceiverStream` from tokio-stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
pub(crate) struct ReceiverStreamBuilder<O> {
    tx: Sender<DeltaResult<O>>,
    rx: Receiver<DeltaResult<O>>,
    join_set: JoinSet<DeltaResult<()>>,
}

impl<O: Send + 'static> ReceiverStreamBuilder<O> {
    /// Create new channels with the specified buffer size
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);

        Self {
            tx,
            rx,
            join_set: JoinSet::new(),
        }
    }

    /// Get a handle for sending data to the output
    pub fn tx(&self) -> Sender<DeltaResult<O>> {
        self.tx.clone()
    }

    /// Spawn task that will be aborted if this builder (or the stream
    /// built from it) are dropped
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = DeltaResult<()>>,
        F: Send + 'static,
    {
        self.join_set.spawn(task);
    }

    /// Spawn a blocking task that will be aborted if this builder (or the stream
    /// built from it) are dropped.
    ///
    /// This is often used to spawn tasks that write to the sender
    /// retrieved from `Self::tx`.
    pub fn spawn_blocking<F>(&mut self, f: F)
    where
        F: FnOnce() -> DeltaResult<()>,
        F: Send + 'static,
    {
        self.join_set.spawn_blocking(f);
    }

    /// Create a stream of all data written to `tx`
    pub fn build(self) -> BoxStream<'static, DeltaResult<O>> {
        let Self { tx, rx, join_set } = self;

        // Doesn't need tx
        drop(tx);

        // Create a stream that checks for task completion
        let check_stream = futures::stream::unfold(join_set, |mut join_set| async move {
            match join_set.join_next().await {
                Some(join_result) => {
                    let task_result = match join_result {
                        Ok(task_result) => task_result,
                        Err(join_error) => {
                            if join_error.is_cancelled() {
                                return None;
                            } else if join_error.is_panic() {
                                Err(DeltaTableError::Generic(format!(
                                    "Task panicked: {}",
                                    join_error
                                )))
                            } else {
                                Err(DeltaTableError::Generic(format!(
                                    "Task failed: {}",
                                    join_error
                                )))
                            }
                        }
                    };

                    // If the task failed, return the error
                    if let Err(e) = task_result {
                        Some((Err(e), join_set))
                    } else {
                        // Task completed successfully, check for more
                        Some((Ok(None), join_set))
                    }
                }
                None => None, // No more tasks
            }
        })
        .filter_map(|result| async move {
            match result {
                Ok(None) => None,                 // Task completed successfully, continue
                Ok(Some(item)) => Some(Ok(item)), // This shouldn't happen in our case
                Err(e) => Some(Err(e)),           // Task failed
            }
        });

        // Convert the receiver into a stream
        let rx_stream = futures::stream::unfold(rx, |mut rx| async move {
            let next_item = rx.recv().await;
            next_item.map(|next_item| (next_item, rx))
        });

        // Merge the streams together so whichever is ready first
        // produces the batch
        futures::stream::select(rx_stream, check_stream).boxed()
    }
}

pub type RecordBatchReceiverStreamBuilder = ReceiverStreamBuilder<RecordBatch>;
