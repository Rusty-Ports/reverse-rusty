use super::{
    as_year, emit_generic, parse_number, position_index, EmitMode, FeatureKind, NormScratch,
    Normalizer, PhraseMode, Side,
};

impl Normalizer {
    /// Core: emit canonical feature names for `text`. Calls `emit(name, kind)`
    /// for each feature found. Shared by compile and match paths so the two
    /// always agree. `lc` is a reusable scratch String; `sc` holds the reusable
    /// per-call working buffers (see [`NormScratch`]) — every one is cleared at
    /// the start, so no state is carried between calls.
    ///
    /// Two-phase approach:
    ///   1) Run the daachorse automaton over the cleaned text to find all
    ///      leftmost-longest multiword phrase matches. Record which byte ranges
    ///      are consumed.
    ///   2) Iterate through tokens. Tokens fully inside a phrase match are
    ///      skipped (the phrase feature is emitted once). All other tokens go
    ///      through the number/synonym/generic pipeline.
    pub fn emit<F: FnMut(&str, FeatureKind)>(
        &self,
        text: &str,
        lc: &mut String,
        sc: &mut NormScratch,
        side: Side,
        force_additive: bool,
        emit: &mut F,
    ) {
        self.emit_positioned(
            text,
            lc,
            sc,
            EmitMode::flat(side, force_additive),
            &mut |name, kind, _start, _end| emit(name, kind),
        );
    }

    /// Positioned twin of [`emit`](Self::emit), used only when at least one
    /// stored quoted clause requires ADR-120 phrase verification. Ordinary
    /// feature-only matching continues to call `emit`, whose wrapper erases the
    /// two position integers after inlining.
    pub(super) fn emit_positioned<F: FnMut(&str, FeatureKind, u32, u32)>(
        &self,
        text: &str,
        lc: &mut String,
        sc: &mut NormScratch,
        mode: EmitMode,
        emit: &mut F,
    ) {
        let EmitMode {
            side,
            force_additive,
            retain_positioned_starts,
        } = mode;
        self.clean_into(text, lc);

        // Phase 1: the phrases in the cleaned text. A phrase is registered as its words joined
        // by single spaces, and cleaning merges separators, so a phrase occurs wherever its
        // words are consecutive tokens (ADR-218).
        //
        // Selection is leftmost-longest over the occurrences that sit on token boundaries
        // (`PhraseOverlap::select_phrases`). The leftmost-longest automaton does the same job
        // faster whenever every match it reports is on token boundaries, which is nearly
        // always, so it runs first. It commits to a match before the boundary is checked: a
        // pattern found inside a word (`north star` in `xnorth star lamp`), or one that ends
        // inside a word (`new york city` in `new york cityscape`), is taken, hides the valid
        // phrases it overlaps (`star lamp`, `new york`), and is then of no use. The first
        // such match hands the text to the boundary-aware selection.
        let phrase_matches = &mut sc.phrase_matches;
        phrase_matches.clear();
        let mut on_boundaries = !(self.has_multiword_aliases || retain_positioned_starts);
        if on_boundaries {
            let bytes = lc.as_bytes();
            for m in self.automaton.leftmost_find_iter(&**lc) {
                let (start, end) = (m.start(), m.end());
                if (start == 0 || bytes[start - 1] == b' ')
                    && (end == lc.len() || bytes[end] == b' ')
                {
                    phrase_matches.push((start, end, m.value()));
                } else {
                    on_boundaries = false;
                    break;
                }
            }
        }
        if !on_boundaries {
            match self.phrase_overlap.as_ref() {
                Some(ov) => ov.select_phrases(lc, phrase_matches),
                None => phrase_matches.clear(),
            }
        }

        // Build a byte-position set of consumed ranges for fast lookup.
        // For each token we'll check if its start byte falls inside a phrase match.
        // Phrase matches are non-overlapping (leftmost-longest), so a sorted list
        // with binary search works.

        // Phase 2: tokenize and iterate, skipping phrase-consumed spans.
        // Token byte-ranges into `lc` (reused buffer): `(start, end)`. This replaces the
        // old `Vec<&str>` (a borrow a reusable buffer cannot hold) AND the separate
        // `token_offsets` — `tokens[i].0` is the offset, `&lc[s..e]` re-slices on demand.
        let tokens = &mut sc.tokens;
        tokens.clear();
        {
            let bytes = lc.as_bytes();
            let mut pos = 0usize;
            while pos < bytes.len() {
                while pos < bytes.len() && bytes[pos] == b' ' {
                    pos += 1;
                }
                if pos >= bytes.len() {
                    break;
                }
                let start = pos;
                while pos < bytes.len() && bytes[pos] != b' ' {
                    pos += 1;
                }
                tokens.push((start, pos));
            }
        }

        // For each token, determine if it's inside a phrase match.
        // If so, emit the phrase feature at the FIRST token of the match (skip rest).
        let phrase_emitted = &mut sc.phrase_emitted;
        phrase_emitted.clear();
        phrase_emitted.resize(phrase_matches.len(), false);
        let token_consumed = &mut sc.token_consumed;
        token_consumed.clear();
        token_consumed.resize(tokens.len(), false);

        for ti in 0..tokens.len() {
            let (tstart, tend) = tokens[ti];
            for (pi, &(ps, pe, _)) in phrase_matches.iter().enumerate() {
                if tstart >= ps && tend <= pe {
                    let entry = &self.phrase_entries[phrase_matches[pi].2];
                    // Additive phrases (corpus-learned, ADR-053) emit the phrase feature but
                    // leave the component tokens for phase 2b, so the component features are
                    // also produced (recall-preserving). Collapse phrases consume them. An
                    // alias phrase (ADR-061) is asymmetric: it collapses on the query side (so
                    // the form reduces to its single entity for ADR-054 expansion) but stays
                    // additive on the title side (so a component query still matches).
                    // `force_additive` (the positive title view `P(T)`, ADR-061) consumes nothing,
                    // so EVERY token also reaches phase 2b — the maximal, parse-union feature set
                    // that keeps a component query matchable even when its phrase is displaced from
                    // the leftmost-longest parse by an overlapping one (codex R7).
                    let consume = !force_additive
                        && match entry.mode {
                            PhraseMode::Collapse => true,
                            PhraseMode::Additive => false,
                            PhraseMode::Alias => side == Side::Query,
                        };
                    if consume {
                        token_consumed[ti] = true;
                    }
                    if !phrase_emitted[pi] {
                        phrase_emitted[pi] = true;
                        let mut end_pos = ti + 1;
                        while end_pos < tokens.len() && tokens[end_pos].1 <= pe {
                            end_pos += 1;
                        }
                        emit(
                            &entry.feature,
                            entry.kind,
                            position_index(ti),
                            position_index(end_pos),
                        );
                    }
                    break;
                }
            }
        }

        // Phase 2b: process non-consumed tokens through the number/synonym/generic
        // pipeline. `scratch` is reused on `sc`; token text is re-sliced from `lc`
        // on demand via `tok_at` (the ranges live in `tokens`).
        let scratch = &mut sc.scratch;
        scratch.clear();
        let tok_at = |r: (usize, usize)| &lc[r.0..r.1];
        let mut i = 0;

        while i < tokens.len() {
            if token_consumed[i] {
                // This token was part of a collapse phrase match.
                i += 1;
                continue;
            }

            let tok = tok_at(tokens[i]);

            // 0) structural markers from cleaning: skip
            if tok == "#" || tok == "/" {
                i += 1;
                continue;
            }

            // 1) Numbers: structural identifiers and caller-declared numeric
            // contexts remain generic; otherwise four-digit years are typed.
            if let Some(numstr) = parse_number(tok) {
                let prev = if i > 0 {
                    Some(tok_at(tokens[i - 1]))
                } else {
                    None
                };
                let next = tokens.get(i + 1).map(|&r| tok_at(r));
                let is_marked_number = prev == Some("#");
                let is_serial = prev == Some("/") || next == Some("/");
                let is_numctx = prev.is_some_and(|p| {
                    self.number_context
                        .iter()
                        .any(|w| p.eq_ignore_ascii_case(w))
                });

                if is_marked_number || is_serial || is_numctx {
                    emit_generic(
                        &numstr,
                        scratch,
                        position_index(i),
                        position_index(i.saturating_add(1)),
                        emit,
                    );
                } else if let Some(y) = as_year(&numstr) {
                    scratch.clear();
                    scratch.push_str("year:");
                    scratch.push_str(&y);
                    emit(
                        scratch,
                        FeatureKind::Year,
                        position_index(i),
                        position_index(i.saturating_add(1)),
                    );
                } else {
                    emit_generic(
                        &numstr,
                        scratch,
                        position_index(i),
                        position_index(i.saturating_add(1)),
                        emit,
                    );
                }
                i += 1;
                continue;
            }

            // 2) closed-vocab synonym
            if let Some(&si) = self.syn_index.get(tok) {
                let (_, canon, kind) = &self.synonyms[si];
                emit(
                    canon,
                    *kind,
                    position_index(i),
                    position_index(i.saturating_add(1)),
                );
                i += 1;
                continue;
            }

            // 3) generic fallback term
            emit_generic(
                tok,
                scratch,
                position_index(i),
                position_index(i.saturating_add(1)),
                emit,
            );
            i += 1;
        }
    }
}
