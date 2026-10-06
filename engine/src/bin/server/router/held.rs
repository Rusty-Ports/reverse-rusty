//! Requests that take slots of the request pool and keep them until the test lets go.
//!
//! Admission gives a request its slots before the handler runs, and the handler asks for
//! the body. A body that reports that first ask and then withholds its bytes therefore
//! marks the moment the slots are taken, without a sleep, and holds them for as long as
//! the test wants.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{Request, Response};
use axum::Router;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower::ServiceExt;

/// The body of a held request: nothing until `release` fires, then the end of the body.
struct WithheldBody {
    asked: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<()>,
}

impl tokio_stream::Stream for WithheldBody {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(asked) = self.asked.take() {
            let _ = asked.send(());
        }
        match Pin::new(&mut self.release).poll(cx) {
            Poll::Ready(_) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A request sent to `router` whose body the test controls.
pub(crate) struct Sent {
    asked: Option<oneshot::Receiver<()>>,
    release: oneshot::Sender<()>,
    response: JoinHandle<Response<Body>>,
}

impl Sent {
    /// Whether the handler asks for the body within `wait`. Once it has, the request was
    /// admitted and holds its slots until it is released.
    pub(crate) async fn is_admitted_within(&mut self, wait: Duration) -> bool {
        let Some(asked) = self.asked.as_mut() else {
            return true;
        };
        if tokio::time::timeout(wait, asked).await.is_err() {
            return false;
        }
        self.asked = None;
        true
    }

    /// End the request's body and wait for its response, which frees its slots.
    pub(crate) async fn release(self) -> Response<Body> {
        let _ = self.release.send(());
        finished(self.response).await
    }
}

/// The response of a released request. Bounded, so a request that is never admitted fails
/// its test instead of hanging it.
async fn finished(response: JoinHandle<Response<Body>>) -> Response<Body> {
    tokio::time::timeout(Duration::from_secs(30), response)
        .await
        .expect("a released request finishes")
        .expect("request task")
}

/// Release every request, then wait for every response. Requests that wait for the same
/// slot are admitted in the order they queued, not the order the test sent them, so each
/// must be free to finish before any is waited for.
pub(crate) async fn release_all(requests: impl IntoIterator<Item = Sent>) {
    let mut responses = Vec::new();
    for request in requests {
        let _ = request.release.send(());
        responses.push(request.response);
    }
    for response in responses {
        finished(response).await;
    }
}

/// Send `method path` with a JSON body that does not arrive until the request is released.
pub(crate) fn send(router: &Router, method: &str, path: &str) -> Sent {
    let (asked, was_asked) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let body = Body::from_stream(WithheldBody {
        asked: Some(asked),
        release: released,
    });
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(body)
        .expect("request");
    let service = router.clone();
    let response =
        tokio::spawn(async move { service.oneshot(request).await.expect("router response") });
    Sent {
        asked: Some(was_asked),
        release,
        response,
    }
}

/// [`send`] a request and return once it is in flight, holding its slots.
pub(crate) async fn hold(router: &Router, method: &str, path: &str) -> Sent {
    let mut sent = send(router, method, path);
    assert!(
        sent.is_admitted_within(Duration::from_secs(30)).await,
        "{method} {path} was never admitted"
    );
    sent
}

/// A bodiless request.
pub(crate) fn bare(method: &str, path: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .expect("request")
}
