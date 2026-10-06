//! The work the completion does for one title (ADR-205): which forms it examines, how
//! often, and what it remembers.

use super::id;
use crate::dict::{Dict, FeatureId};

/// Run the completion on a title given by the names it carries. Returns the entities added,
/// distinct and in order, and how many times a form was examined.
fn complete(
    words: &super::super::core::AliasWords,
    carried: &mut Vec<u64>,
) -> (Vec<FeatureId>, usize) {
    let mut scratch = super::super::core::AliasScratch::default();
    let (out, examined) = complete_with(words, carried, &mut scratch);
    (out, examined)
}

fn complete_with(
    words: &super::super::core::AliasWords,
    carried: &mut Vec<u64>,
    scratch: &mut super::super::core::AliasScratch,
) -> (Vec<FeatureId>, usize) {
    let mut out = Vec::new();
    scratch.names.clear();
    scratch.names.append(carried);
    let examined = words.complete_into(scratch, &Dict::new(), &mut out);
    carried.clone_from(&scratch.names);
    // The view sorts what it is given, and a form can be reached twice.
    out.sort_unstable();
    out.dedup();
    (out, examined)
}

fn hashes(names: &[&str]) -> Vec<u64> {
    names
        .iter()
        .map(|name| super::super::core::name_hash(name))
        .collect()
}

fn ids(names: &[&str]) -> Vec<FeatureId> {
    let mut ids: Vec<FeatureId> = names.iter().map(|name| id(name)).collect();
    ids.sort_unstable();
    ids
}

/// A form of single words, each carried under one name.
fn form(entity: &str, words: &[&str]) -> super::super::core::FormSpec {
    let named: Vec<Vec<&str>> = words.iter().map(|word| vec![*word]).collect();
    form_of(entity, &named)
}

/// A form of single words, each carried under any of its names.
fn form_of(entity: &str, words: &[Vec<&str>]) -> super::super::core::FormSpec {
    super::super::core::FormSpec {
        entity: entity.to_string(),
        len: words.len(),
        units: words
            .iter()
            .enumerate()
            .map(|(at, names)| super::super::core::UnitSpec {
                from: at,
                to: at + 1,
                names: names.iter().map(ToString::to_string).collect(),
            })
            .collect(),
    }
}

#[test]
fn a_title_that_repeats_a_word_carries_it_once() {
    // The names of a title are reduced to the distinct ones before any form is examined,
    // so a word repeated thousands of times costs one look, not one per occurrence.
    let words = super::super::core::AliasWords::new(vec![form(
        "term:wireless_mouse",
        &["term:wireless", "term:mouse"],
    )])
    .expect("alias words");
    let wireless = super::super::core::name_hash("term:wireless");
    let mouse = super::super::core::name_hash("term:mouse");

    let mut carried = vec![wireless; 50_000];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(carried, vec![wireless]);
    assert!(out.is_empty());
    assert!(examined <= 1, "examined {examined} times");

    let mut carried = vec![mouse, wireless, mouse, wireless];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(out, vec![id("term:wireless_mouse")]);
    assert_eq!(examined, 1);
}

#[test]
fn the_work_follows_the_forms_a_title_touches() {
    // Thousands of forms that share a word: a title with that word alone examines none.
    let shared: Vec<_> = (0..5_000)
        .map(|model| {
            form(
                &format!("term:wireless_m{model}"),
                &["term:wireless", &format!("term:m{model}")],
            )
        })
        .collect();
    let words = super::super::core::AliasWords::new(shared).expect("alias words");
    let (out, examined) = complete(
        &words,
        &mut vec![super::super::core::name_hash("term:wireless")],
    );
    assert!(out.is_empty());
    assert_eq!(examined, 0);

    // A chain two thousand forms long, each built on the entity of the one before. The
    // title carries the first word; every form completes, and each is examined once.
    let chain: Vec<_> = (0..2_000)
        .map(|link| {
            form(
                &format!("term:s{}", link + 1),
                &[&format!("term:s{link}"), "term:unit"],
            )
        })
        .collect();
    let words = super::super::core::AliasWords::new(chain).expect("alias words");
    let mut carried = vec![
        super::super::core::name_hash("term:unit"),
        super::super::core::name_hash("term:s0"),
    ];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(out.len(), 2_000);
    assert!(out.contains(&id("term:s2000")));
    assert_eq!(examined, 2_000);

    // The chain entered in the middle completes from there on only.
    let mut carried = vec![
        super::super::core::name_hash("term:unit"),
        super::super::core::name_hash("term:s1500"),
    ];
    let (out, examined) = complete(&words, &mut carried);
    assert_eq!(out.len(), 500);
    assert_eq!(examined, 500);

    // An entity the title already carries has had its forms examined; completing the
    // form for it does not examine them again.
    let (out, examined) = complete(
        &words,
        &mut hashes(&["term:unit", "term:s0", "term:s1", "term:s2"]),
    );
    assert_eq!(out.len(), 2_000);
    assert_eq!(examined, 2_000);
}

#[test]
fn two_forms_for_one_entity_add_it_once() {
    let words = super::super::core::AliasWords::new(vec![
        form("entity:place", &["term:new", "term:york"]),
        form("entity:place", &["term:big", "term:apple"]),
        form("term:trip", &["entity:place", "term:tour"]),
    ])
    .expect("alias words");
    let mut carried: Vec<u64> = [
        "term:new",
        "term:york",
        "term:big",
        "term:apple",
        "term:tour",
    ]
    .iter()
    .map(|name| super::super::core::name_hash(name))
    .collect();
    let (out, _) = complete(&words, &mut carried);
    assert_eq!(out, ids(&["entity:place", "term:trip"]));
}

#[test]
fn an_entity_that_many_forms_share_as_a_word_wakes_none_of_them() {
    // `wire less` is a form for the word `wireless`, which ten thousand forms
    // `wireless <model>` share. They are keyed on their model, so a title that carries
    // `wireless` only through `wire less` examines the one form it touches.
    let mut forms = vec![form("term:wireless", &["term:wire", "term:less"])];
    forms.extend((0..10_000).map(|model| {
        form(
            &format!("term:wireless_m{model}"),
            &["term:wireless", &format!("term:m{model}")],
        )
    }));
    let words = super::super::core::AliasWords::new(forms).expect("alias words");
    assert_eq!(words.forms_keyed_on("term:wireless"), 0);

    let (out, examined) = complete(&words, &mut hashes(&["term:wire", "term:less"]));
    assert_eq!(out, ids(&["term:wireless"]));
    assert_eq!(examined, 1);

    // With a model as well, that model's form is the only other one looked at: once if
    // `wireless` was already in the view, twice if the form had to wait for it.
    let (out, examined) = complete(&words, &mut hashes(&["term:wire", "term:less", "term:m7"]));
    assert_eq!(out, ids(&["term:wireless", "term:wireless_m7"]));
    assert!((2..=3).contains(&examined), "examined {examined} times");
}

#[test]
fn a_form_waits_for_the_word_it_lacks() {
    // `zk s2` is keyed on `zk`, which the title carries, and lacks `s2`, which only the
    // completion can supply, two steps later. It is looked at once when its key is seen
    // and once when `s2` arrives, and not in between.
    let words = super::super::core::AliasWords::new(vec![
        form("term:s3", &["term:zk", "term:s2"]),
        form("term:s1", &["term:za", "term:zb"]),
        form("term:s2", &["term:s1", "term:zc"]),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:zk"), 1);
    assert_eq!(words.forms_keyed_on("term:s2"), 0);
    let (out, examined) = complete(
        &words,
        &mut hashes(&["term:za", "term:zb", "term:zc", "term:zk"]),
    );
    assert_eq!(out, ids(&["term:s1", "term:s2", "term:s3"]));
    assert_eq!(examined, 4);

    // Without the word the chain needs, the waiting form is never looked at again.
    let (out, examined) = complete(&words, &mut hashes(&["term:za", "term:zb", "term:zk"]));
    assert_eq!(out, ids(&["term:s1"]));
    assert_eq!(examined, 3, "each form once");
}

#[test]
fn a_form_is_noted_once_under_a_word_it_waits_on() {
    // The second word of `last` can arrive under two names, `a1` and `a2`, and both do,
    // one after the other, while its third word `b` is still two steps away. The second
    // arrival finds the form waiting on `b` as the first left it, and notes nothing: when
    // `b` arrives the form is looked at once, not once per earlier look.
    let twice = |entity: &str, word: &str| form(entity, &[word, word]);
    let words = super::super::core::AliasWords::new(vec![
        form("term:q1", &["term:x1", "term:x2"]),
        form("term:q2", &["term:y1", "term:y2"]),
        twice("term:a1", "term:q1"),
        twice("term:a2", "term:q2"),
        form("term:bb", &["term:a1", "term:a2"]),
        twice("term:b", "term:bb"),
        form_of(
            "term:last",
            &[
                vec!["term:zkey"],
                vec!["term:a1", "term:a2"],
                vec!["term:b"],
            ],
        ),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:zkey"), 1);
    let (out, examined) = complete(
        &words,
        &mut hashes(&["term:x1", "term:x2", "term:y1", "term:y2", "term:zkey"]),
    );
    assert_eq!(
        out,
        ids(&[
            "term:q1",
            "term:q2",
            "term:a1",
            "term:a2",
            "term:bb",
            "term:b",
            "term:last"
        ])
    );
    // Six forms once each, and `last` four times: at its key, when `a1` and `a2` arrive,
    // and when `b` does.
    assert_eq!(examined, 10);
}

#[test]
fn a_key_word_carried_under_two_names_completes_the_form_once() {
    // `unit` is a word of two forms, so the first is keyed on its other word, which has
    // two names.
    let words = super::super::core::AliasWords::new(vec![
        form_of(
            "term:refurb_unit",
            &[vec!["term:refurb", "term:refurbished"], vec!["term:unit"]],
        ),
        form("term:unit_price", &["term:unit", "term:price"]),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:refurb"), 1);
    assert_eq!(words.forms_keyed_on("term:refurbished"), 1);
    let mut scratch = super::super::core::AliasScratch::default();
    let mut out = Vec::new();
    scratch.names = hashes(&["term:refurb", "term:refurbished", "term:unit"]);
    let examined = words.complete_into(&mut scratch, &Dict::new(), &mut out);
    assert_eq!(out, vec![id("term:refurb_unit")], "added once");
    assert_eq!(examined, 2, "once per name of the key word");
}

#[test]
fn a_title_that_touches_no_form_holds_no_completion_state() {
    // A scratch made for one title (cluster routing makes one per request) must not pay
    // for the size of the registry.
    let forms: Vec<_> = (0..20_000)
        .map(|model| {
            form(
                &format!("term:wireless_m{model}"),
                &["term:wireless", &format!("term:m{model}")],
            )
        })
        .collect();
    let words = super::super::core::AliasWords::new(forms).expect("alias words");
    let mut scratch = super::super::core::AliasScratch::default();
    let (out, examined) = complete_with(
        &words,
        &mut hashes(&["term:wireless", "term:lamp"]),
        &mut scratch,
    );
    assert!(out.is_empty());
    assert_eq!(examined, 0);
    assert_eq!(scratch.held(), 0);

    // Nor does a title that has a form's key word and lacks a word nothing can supply.
    let (out, examined) = complete_with(&words, &mut hashes(&["term:m3"]), &mut scratch);
    assert!(out.is_empty());
    assert_eq!(examined, 1);
    assert_eq!(scratch.held(), 0);
}

#[test]
fn nothing_of_one_title_is_left_for_the_next() {
    // One scratch serves every title of a request. `s3` waits on `s2`; `s2` is keyed on
    // `zc` and needs `s1`.
    let words = super::super::core::AliasWords::new(vec![
        form("term:s1", &["term:za", "term:zb"]),
        form("term:s2", &["term:zc", "term:s1"]),
        form("term:s3", &["term:zk", "term:s2"]),
    ])
    .expect("alias words");
    let mut scratch = super::super::core::AliasScratch::default();
    let full = ["term:za", "term:zb", "term:zc", "term:zk"];
    let all = ids(&["term:s1", "term:s2", "term:s3"]);

    let (out, first) = complete_with(&words, &mut hashes(&full), &mut scratch);
    assert_eq!(out, all);
    // The same title again completes the same forms with the same work: a form that was
    // completed for the first title is not taken as done for the second.
    let (out, again) = complete_with(&words, &mut hashes(&full), &mut scratch);
    assert_eq!(out, all);
    assert_eq!(again, first);

    // `s1` was in the first title's view. A title without its words does not carry it,
    // so `s2` is examined and not completed, and `s3` waits on a word that never comes.
    let (out, examined) = complete_with(&words, &mut hashes(&["term:zc", "term:zk"]), &mut scratch);
    assert!(out.is_empty());
    assert_eq!(examined, 2);

    // A title that left `s3` waiting leaves no one waiting for the next title.
    let (out, examined) = complete_with(&words, &mut hashes(&["term:za", "term:zb"]), &mut scratch);
    assert_eq!(out, ids(&["term:s1"]));
    assert_eq!(examined, 1);
}

/// A dictionary in which the given names are one class to a query.
fn dict_with_class(names: &[&str]) -> Dict {
    // Sorted, as `Vocab::resolve_equivalences` builds a class.
    let mut class = hashes(names);
    class.sort_unstable();
    class.dedup();
    let class: std::sync::Arc<[u64]> = class.into();
    let mut by_name = crate::util::fast_map();
    for &name in class.iter() {
        by_name.insert(name, std::sync::Arc::clone(&class));
    }
    let mut dict = Dict::new();
    dict.set_equivalences(crate::dict::Equivalences::new(
        crate::util::fast_map(),
        by_name,
    ));
    dict
}

#[test]
fn a_form_waits_for_a_piece_an_equivalent_will_supply() {
    // `zk x` is keyed on `zk`, which the title carries, and lacks `x`. No form has `x` for
    // its entity, but a query takes `x` and `eb` for each other, and `eb` is the entity of
    // a form that completes two steps later. The form waits for `x` and is looked at again
    // when `eb` brings it.
    let words = super::super::core::AliasWords::new(vec![
        form("term:ew", &["term:zk", "term:x"]),
        form("term:ea", &["term:za", "term:zb"]),
        form("term:eb", &["term:ea", "term:zc"]),
    ])
    .expect("alias words");
    assert_eq!(words.forms_keyed_on("term:zk"), 1);
    let dict = dict_with_class(&["term:eb", "term:x"]);
    let mut scratch = super::super::core::AliasScratch::default();
    let mut out = Vec::new();
    scratch.names = hashes(&["term:za", "term:zb", "term:zc", "term:zk"]);
    let examined = words.complete_into(&mut scratch, &dict, &mut out);
    out.sort_unstable();
    assert_eq!(out, ids(&["term:ea", "term:eb", "term:ew"]));
    assert_eq!(
        examined, 4,
        "`ew` at its key and once more when `x` arrives"
    );

    // The same title again, with the same scratch: the class `eb` brought into the first
    // title's view is not taken to be in this one's.
    scratch.names = hashes(&["term:za", "term:zb", "term:zc", "term:zk"]);
    out.clear();
    let again = words.complete_into(&mut scratch, &dict, &mut out);
    out.sort_unstable();
    assert_eq!(out, ids(&["term:ea", "term:eb", "term:ew"]));
    assert_eq!(again, examined);

    // A title that carries `eb`'s equivalent carries `x` from the start.
    scratch.names = hashes(&["term:zk", "term:eb"]);
    out.clear();
    let examined = words.complete_into(&mut scratch, &dict, &mut out);
    assert_eq!(out, ids(&["term:ew"]));
    assert_eq!(examined, 1);

    // And the class that title carried is not taken to be in the next one's view.
    scratch.names = hashes(&["term:za", "term:zb", "term:zc", "term:zk"]);
    out.clear();
    words.complete_into(&mut scratch, &dict, &mut out);
    out.sort_unstable();
    assert_eq!(out, ids(&["term:ea", "term:eb", "term:ew"]));
}

#[test]
fn a_class_is_added_once_however_many_of_its_members_a_title_carries() {
    // Two thousand names a query takes for each other, and a title that carries all of
    // them. The view is widened by the class once, not once per member it carries.
    let members: Vec<String> = (0..2_000).map(|at| format!("term:zzsame{at}")).collect();
    let members: Vec<&str> = members.iter().map(String::as_str).collect();
    let dict = dict_with_class(&members);
    let words = super::super::core::AliasWords::new(vec![form(
        "term:zzother_form",
        &["term:zzother", "term:zzform"],
    )])
    .expect("alias words");
    let mut scratch = super::super::core::AliasScratch::default();
    let mut out = Vec::new();
    scratch.names = hashes(&members);
    let examined = words.complete_into(&mut scratch, &dict, &mut out);
    assert_eq!(examined, 0);
    assert!(out.is_empty());
    assert_eq!(scratch.names.len(), members.len());
    assert!(
        scratch.names.capacity() < 4 * members.len(),
        "the view grew to {} names for a class of {}",
        scratch.names.capacity(),
        members.len()
    );

    // One member is enough to carry the whole class.
    scratch.names = hashes(&members[..1]);
    words.complete_into(&mut scratch, &dict, &mut out);
    assert_eq!(scratch.names.len(), members.len());
}

#[test]
fn what_a_class_can_supply_is_decided_afresh_for_each_title() {
    // `x` is a piece of two forms. In the first dictionary a query takes `x` for a name
    // that is no form's entity, so nothing can supply it and nothing waits. In the second
    // the same class also holds `eb`, the entity of a form that completes for this title,
    // and both forms get `x` from it. One scratch serves both, as it does when a
    // vocabulary changes under a running server.
    let words = super::super::core::AliasWords::new(vec![
        form("term:ew1", &["term:zk1", "term:x"]),
        form("term:ew2", &["term:zk2", "term:x"]),
        form("term:ea", &["term:za", "term:zb"]),
        form("term:eb", &["term:ea", "term:zc"]),
    ])
    .expect("alias words");
    // A class is told by its first member. Take a member that sorts before `eb`, so that
    // the two classes are told by the same one.
    let low = (0..10_000)
        .map(|at| format!("term:zzlow{at}"))
        .find(|name| crate::dict::name_hash(name) < crate::dict::name_hash("term:eb"))
        .expect("a name that sorts first");
    let carried = ["term:za", "term:zb", "term:zc", "term:zk1", "term:zk2"];
    let mut scratch = super::super::core::AliasScratch::default();
    let mut out = Vec::new();

    let without = dict_with_class(&["term:x", &low]);
    scratch.names = hashes(&carried);
    let examined = words.complete_into(&mut scratch, &without, &mut out);
    out.sort_unstable();
    assert_eq!(out, ids(&["term:ea", "term:eb"]));
    assert_eq!(examined, 4, "each form once");

    let with = dict_with_class(&["term:eb", "term:x", &low]);
    scratch.names = hashes(&carried);
    out.clear();
    words.complete_into(&mut scratch, &with, &mut out);
    out.sort_unstable();
    assert_eq!(out, ids(&["term:ea", "term:eb", "term:ew1", "term:ew2"]));
}

#[test]
fn a_class_is_gone_through_once_however_many_forms_complete_into_it() {
    // Two thousand forms with one entity, which a query takes for two thousand other
    // names. A title that completes every form goes through the class once.
    let forms: Vec<_> = (0..2_000)
        .map(|at| form("term:zzone", &[&format!("term:zzv{at}"), "term:zzcommon"]))
        .collect();
    let words = super::super::core::AliasWords::new(forms).expect("alias words");
    let mut members: Vec<String> = (0..2_000).map(|at| format!("term:zzalso{at}")).collect();
    members.push("term:zzone".to_string());
    let members: Vec<&str> = members.iter().map(String::as_str).collect();
    let dict = dict_with_class(&members);

    let mut carried: Vec<String> = (0..2_000).map(|at| format!("term:zzv{at}")).collect();
    carried.push("term:zzcommon".to_string());
    let carried: Vec<&str> = carried.iter().map(String::as_str).collect();
    let mut scratch = super::super::core::AliasScratch::default();
    let mut out = Vec::new();
    scratch.names = hashes(&carried);
    let examined = words.complete_into(&mut scratch, &dict, &mut out);
    assert_eq!(examined, 2_000);
    out.sort_unstable();
    out.dedup();
    assert_eq!(out, ids(&["term:zzone"]));
    assert_eq!(
        scratch.scanned,
        members.len(),
        "the class was gone through once"
    );

    // A class the title's own names already brought in is not gone through again.
    scratch.scanned = 0;
    let mut carried = carried.clone();
    carried.push("term:zzalso7");
    scratch.names = hashes(&carried);
    out.clear();
    words.complete_into(&mut scratch, &dict, &mut out);
    assert_eq!(scratch.scanned, 0);
    out.sort_unstable();
    out.dedup();
    assert_eq!(out, ids(&["term:zzone"]));
}
