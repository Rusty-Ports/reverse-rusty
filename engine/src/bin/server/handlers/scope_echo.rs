//! The scope a compatibility request was evaluated in, echoed as a response header.
//!
//! `/_search` and `/_mpercolate` take an optional `include_broad` and fall back to the
//! server's default, and their bodies do not say which was used (ADR-107 keeps those bytes
//! unchanged). A header does, without touching the body. The v2 routes and job status carry
//! `query_scope` in the body already.

use axum::http::{HeaderName, HeaderValue};
use axum::response::{IntoResponse, Response};
use reverse_rusty::QueryScope;

/// `standard` or `with_broad`: the scope the response's hits were matched in.
pub(crate) const QUERY_SCOPE_HEADER: HeaderName = HeaderName::from_static("x-rr-query-scope");

/// The scope of a compatibility request: what it asked for, or the server's default.
pub(crate) fn effective_scope(
    include_broad: Option<bool>,
    server_default: QueryScope,
) -> QueryScope {
    match include_broad {
        Some(true) => QueryScope::WithBroad,
        Some(false) => QueryScope::Standard,
        None => server_default,
    }
}

/// A response with the scope it was evaluated in.
pub(crate) struct Scoped<T>(pub(crate) QueryScope, pub(crate) T);

impl<T: IntoResponse> IntoResponse for Scoped<T> {
    fn into_response(self) -> Response {
        let mut response = self.1.into_response();
        let scope = match self.0 {
            QueryScope::Standard => "standard",
            QueryScope::WithBroad => "with_broad",
        };
        response
            .headers_mut()
            .insert(QUERY_SCOPE_HEADER, HeaderValue::from_static(scope));
        response
    }
}
