//! Chunked output plumbing for renderers that can emit rows as they go.
//!
//! A renderer produces a sequence of pieces (header, rows, separators,
//! footer); [`to_stream`] concatenates them into chunks of roughly
//! [`CHUNK_BYTES`]. The HTTP layer forwards each chunk without ever holding the
//! whole document, which is the last full-size copy of a large result.

use crate::content_type::ContentType;
use anyhow::Result;
use bytes::Bytes;
use futures::stream::Stream;
use std::borrow::Cow;
use std::pin::Pin;

/// A response body produced lazily by a renderer, as byte chunks for the HTTP
/// layer.
pub type BodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// A streamed body plus the content type it was rendered as.
pub struct StreamResponse {
    pub stream: BodyStream,
    pub content_type: ContentType,
}

impl std::fmt::Debug for StreamResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamResponse")
            .field("content_type", &self.content_type)
            .finish_non_exhaustive()
    }
}

/// How much text to accumulate before yielding a chunk. Large enough that the
/// streaming overhead is negligible, small enough that a chunk never adds a
/// meaningful peak.
pub const CHUNK_BYTES: usize = 64 * 1024;

/// One rendered piece of the document. Borrowed values are separators and
/// literal markup; owned values are rendered rows.
pub type Piece = Cow<'static, str>;

/// Concatenates `pieces` into chunks of about [`CHUNK_BYTES`].
pub fn to_stream<I>(pieces: I) -> BodyStream
where
    I: Iterator<Item = Piece> + Send + 'static,
{
    let chunks = Chunks {
        pieces,
        buffer: String::with_capacity(CHUNK_BYTES),
        done: false,
    };
    Box::pin(futures::stream::iter(
        chunks.map(|chunk| Ok::<_, std::io::Error>(Bytes::from(chunk))),
    ))
}

struct Chunks<I> {
    pieces: I,
    buffer: String,
    done: bool,
}

impl<I: Iterator<Item = Piece>> Iterator for Chunks<I> {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.done && self.buffer.is_empty() {
            return None;
        }
        self.buffer.clear();
        while self.buffer.len() < CHUNK_BYTES {
            match self.pieces.next() {
                Some(piece) => self.buffer.push_str(&piece),
                None => {
                    self.done = true;
                    break;
                }
            }
        }
        if self.buffer.is_empty() && self.done {
            None
        } else {
            Some(std::mem::take(&mut self.buffer))
        }
    }
}

/// Yields `lines` with `"\n"` between them, matching `lines.join("\n")`.
///
/// The separator is a borrowed piece, so a row is not copied just to prepend
/// a newline. No trailing separator is emitted.
pub fn newline_separated<I>(lines: I) -> impl Iterator<Item = Piece>
where
    I: Iterator<Item = Piece>,
{
    separated(lines, "\n")
}

/// Yields `","` between pieces, for a streamed JSON array.
pub fn comma_separated<I>(items: I) -> impl Iterator<Item = Piece>
where
    I: Iterator<Item = Piece>,
{
    separated(items, ",")
}

fn separated<I>(mut items: I, separator: &'static str) -> impl Iterator<Item = Piece>
where
    I: Iterator<Item = Piece>,
{
    let mut next = items.next();
    let mut emit_separator = false;
    std::iter::from_fn(move || {
        if emit_separator {
            emit_separator = false;
            return Some(Cow::Borrowed(separator));
        }
        let item = next.take()?;
        next = items.next();
        // The separator is emitted before the following item, so there is
        // never a trailing one.
        emit_separator = next.is_some();
        Some(item)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    fn pieces(values: &[&'static str]) -> std::vec::IntoIter<Piece> {
        values
            .iter()
            .map(|value| Cow::Borrowed(*value))
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[tokio::test]
    async fn small_stream_is_one_chunk() {
        let stream = to_stream(pieces(&["a", "b", "c"]));
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks, vec![Bytes::from_static(b"abc")]);
    }

    #[tokio::test]
    async fn stream_splits_at_the_chunk_size() {
        let long = "x".repeat(CHUNK_BYTES + 1);
        let stream = to_stream(pieces(&[Box::leak(long.into_boxed_str()), "y"]));
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), CHUNK_BYTES + 1);
        assert_eq!(chunks[1], Bytes::from_static(b"y"));
    }

    #[tokio::test]
    async fn empty_stream_yields_nothing() {
        let stream = to_stream(pieces(&[]));
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn newline_separated_matches_join() {
        let joined: String = newline_separated(pieces(&["a", "b", "cc"])).collect();
        assert_eq!(joined, "a\nb\ncc");
    }

    #[test]
    fn comma_separated_matches_join() {
        let joined: String = comma_separated(pieces(&["1", "2", "3"])).collect();
        assert_eq!(joined, "1,2,3");
    }
}
