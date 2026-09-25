//! The Porter stemmer matches Martin Porter's reference implementation.

use operon_text::porter_stem;

#[test]
fn porter_matches_the_reference_vocabulary() {
    let vocabulary = include_str!("data/porter_voc.txt");
    let output = include_str!("data/porter_output.txt");
    assert_eq!(vocabulary.lines().count(), 23_531);
    assert_eq!(output.lines().count(), 23_531);
    let mismatches: Vec<String> = vocabulary
        .lines()
        .zip(output.lines())
        .filter(|(word, stem)| porter_stem(word) != *stem)
        .map(|(word, stem)| format!("{word}: expected {stem}, got {}", porter_stem(word)))
        .collect();
    assert!(
        mismatches.is_empty(),
        "{} mismatches, first: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(10)]
    );
}

#[test]
fn porter_known_stems() {
    for (word, stem) in [
        ("caresses", "caress"),
        ("ponies", "poni"),
        ("cats", "cat"),
        ("hopping", "hop"),
        ("generalizations", "gener"),
        ("oscillators", "oscil"),
        ("is", "is"),
    ] {
        assert_eq!(porter_stem(word), stem, "{word}");
    }
    // Words of at most two chars and non-letters pass through.
    assert_eq!(porter_stem("as"), "as");
    assert_eq!(porter_stem("3.5"), "3.5");
    assert_eq!(porter_stem(""), "");
    // A long run of `y`s does not recurse per letter.
    let ys = "y".repeat(100_000);
    assert!(porter_stem(&ys).len() <= ys.len());
}
