//! The single task that owns a link's write half.

use std::future::poll_fn;
use std::io;
use std::task::Poll;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::frame::{CallId, Frame, encode};

type Registration = (CallId, mpsc::Receiver<Frame>);

/// Queues frames for the writer task. Cloning is cheap.
#[derive(Debug, Clone)]
pub struct LinkSender {
    link: mpsc::UnboundedSender<Frame>,
    calls: mpsc::UnboundedSender<Registration>,
}

impl LinkSender {
    pub fn send_link(&self, frame: Frame) {
        let _ = self.link.send(frame);
    }

    #[must_use]
    pub fn call_slot(&self, id: CallId) -> CallSlot {
        let (tx, rx) = mpsc::channel(1);
        let _ = self.calls.send((id, rx));
        CallSlot { tx }
    }
}

/// The one-frame outbound slot of a call. Frames sent through it leave in order.
#[derive(Debug, Clone)]
pub struct CallSlot {
    tx: mpsc::Sender<Frame>,
}

impl CallSlot {
    /// Waits until the slot is free, then fills it. False when the writer is gone.
    pub async fn send(&self, frame: Frame) -> bool {
        self.tx.send(frame).await.is_ok()
    }
}

/// Starts the writer task on `out`.
#[must_use]
pub fn spawn<W: AsyncWrite + Unpin + Send + 'static>(
    out: W,
) -> (LinkSender, JoinHandle<io::Result<()>>) {
    let (link_tx, link_rx) = mpsc::unbounded_channel();
    let (calls_tx, calls_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run(out, link_rx, calls_rx));
    let sender = LinkSender {
        link: link_tx,
        calls: calls_tx,
    };
    (sender, task)
}

async fn next_call_frame(slots: &mut Vec<Registration>, cursor: &mut usize) -> Option<Frame> {
    poll_fn(|cx| {
        let count = slots.len();
        if count == 0 {
            return Poll::Ready(None);
        }
        let start = *cursor % count;
        let mut closed = Vec::new();
        let mut found = None;
        for step in 0..count {
            let index = (start + step) % count;
            let (_, rx) = &mut slots[index];
            match rx.poll_recv(cx) {
                Poll::Ready(Some(frame)) => {
                    *cursor = index + 1;
                    found = Some(frame);
                    break;
                }
                Poll::Ready(None) => closed.push(index),
                Poll::Pending => {}
            }
        }
        closed.sort_unstable();
        for index in closed.into_iter().rev() {
            slots.remove(index);
        }
        match found {
            Some(frame) => Poll::Ready(Some(frame)),
            None if slots.is_empty() => Poll::Ready(None),
            None => Poll::Pending,
        }
    })
    .await
}

async fn run<W: AsyncWrite + Unpin>(
    mut out: W,
    mut link_rx: mpsc::UnboundedReceiver<Frame>,
    mut calls_rx: mpsc::UnboundedReceiver<Registration>,
) -> io::Result<()> {
    let mut slots: Vec<Registration> = Vec::new();
    let mut cursor = 0;
    loop {
        while let Ok(registration) = calls_rx.try_recv() {
            slots.push(registration);
        }
        let frame = tokio::select! {
            biased;
            Some(frame) = link_rx.recv() => frame,
            Some(frame) = next_call_frame(&mut slots, &mut cursor), if !slots.is_empty() => frame,
            Some(registration) = calls_rx.recv() => {
                slots.push(registration);
                continue;
            }
            else => return Ok(()),
        };
        out.write_all(&encode(&frame)).await?;
        out.flush().await?;
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{BufReader, duplex};

    use super::*;
    use crate::frame::{Stream, read_frame};

    fn chunk(id: u64, byte: u8) -> Frame {
        Frame::Chunk {
            id: CallId(id),
            stream: Stream::Stdout,
            data: vec![byte],
        }
    }

    async fn read_n(reader: &mut BufReader<tokio::io::DuplexStream>, count: usize) -> Vec<Frame> {
        let mut frames = Vec::new();
        for _ in 0..count {
            let frame = read_frame(reader)
                .await
                .expect("frame reads")
                .expect("stream is open");
            frames.push(frame);
        }
        frames
    }

    #[tokio::test]
    async fn frames_of_one_call_keep_their_order() {
        let (near, far) = duplex(1024);
        let (sender, _task) = spawn(near);
        let slot = sender.call_slot(CallId(1));
        let producer = tokio::spawn(async move {
            for byte in 0..50 {
                assert!(slot.send(chunk(1, byte)).await);
            }
            assert!(
                slot.send(Frame::Exit {
                    id: CallId(1),
                    code: 0
                })
                .await
            );
        });
        let mut reader = BufReader::new(far);
        let frames = read_n(&mut reader, 51).await;
        producer.await.expect("producer finishes");
        for (index, frame) in frames.iter().take(50).enumerate() {
            let expected = chunk(1, u8::try_from(index).expect("small index"));
            assert_eq!(frame, &expected);
        }
        assert_eq!(
            frames[50],
            Frame::Exit {
                id: CallId(1),
                code: 0
            }
        );
    }

    #[tokio::test]
    async fn link_frames_overtake_call_frames() {
        let (near, far) = duplex(1 << 16);
        let (sender, _task) = spawn(near);
        let slot = sender.call_slot(CallId(1));
        assert!(slot.send(chunk(1, 1)).await);
        sender.send_link(Frame::Ping);
        let mut reader = BufReader::new(far);
        let frames = read_n(&mut reader, 2).await;
        assert_eq!(frames[0], Frame::Ping);
        assert_eq!(frames[1], chunk(1, 1));
    }

    #[tokio::test]
    async fn a_small_call_gets_ahead_of_a_large_one_by_at_most_one_frame() {
        let (near, far) = duplex(256);
        let (sender, _task) = spawn(near);
        let big = sender.call_slot(CallId(1));
        let small = sender.call_slot(CallId(2));
        let big_task = tokio::spawn(async move {
            for byte in 0..100 {
                assert!(big.send(chunk(1, byte)).await);
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(small.send(chunk(2, 0)).await);
        let mut reader = BufReader::new(far);
        let frames = read_n(&mut reader, 101).await;
        big_task.await.expect("big producer finishes");
        let mut position = None;
        for (index, frame) in frames.iter().enumerate() {
            if frame == &chunk(2, 0) {
                position = Some(index);
                break;
            }
        }
        let position = position.expect("the small call's frame is written");
        assert!(position <= 8, "small frame waited behind {position} frames");
    }

    #[tokio::test]
    async fn a_failed_write_ends_the_task_and_frees_senders() {
        let (near, far) = duplex(64);
        drop(far);
        let (sender, task) = spawn(near);
        let slot = sender.call_slot(CallId(1));
        assert!(slot.send(chunk(1, 1)).await);
        let result = task.await.expect("task does not panic");
        assert!(result.is_err());
        assert!(!slot.send(chunk(1, 2)).await);
    }
}
