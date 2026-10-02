//! Martin Porter's 1980 stemmer, ported line by line from his ANSI C
//! reference implementation (`porter.c`, release 3, 25 Mar 2014,
//! <https://tartarus.org/martin/PorterStemmer/>; "free of charge for any
//! purpose", see `NOTICE`), including its three points of DEPARTURE from the
//! published paper (`bli`, `logi`, and words of at most two letters).
//!
//! This is the original Porter algorithm that Lucene's `PorterStemFilter`
//! implements, not Porter2 (Snowball `english`). `tests/porter.rs` checks it
//! against Porter's published 23 531-word vocabulary and output.
//!
//! The C code works on bytes; this port works on `char`s. Only `a e i o u`
//! (and `y` after a consonant) are vowels, so every other `char` is a
//! consonant. The C recursion in `cons` for runs of `y` is unrolled (it is
//! the same function), so no input can overflow the stack.

/// The stem of `word`, which should already be lowercase. Words of at most
/// two `char`s are returned unchanged.
pub fn porter_stem(word: &str) -> String {
    let b: Vec<char> = word.chars().collect();
    if b.len() <= 2 {
        // -DEPARTURE-: strings of length 1 or 2 don't go through the
        // stemming process.
        return word.to_string();
    }
    let mut stemmer = Stemmer {
        k: b.len() as isize - 1,
        j: 0,
        b,
    };
    stemmer.step1ab();
    if stemmer.k > 0 {
        stemmer.step1c();
        stemmer.step2();
        stemmer.step3();
        stemmer.step4();
        stemmer.step5();
    }
    stemmer.b[..=stemmer.k as usize].iter().collect()
}

/// The C statics: the letters are `b[0] ..= b[k]` (`k0` is always 0), and
/// `j` is a general offset into the word, set by [`Stemmer::ends`].
struct Stemmer {
    b: Vec<char>,
    k: isize,
    j: isize,
}

// The `match`es mirror the C `switch`es case for case, so their one-`if`
// arms are kept rather than folded into guards.
#[allow(clippy::collapsible_match)]
impl Stemmer {
    fn at(&self, i: isize) -> char {
        self.b[i as usize]
    }

    /// `cons(i)` is true <=> `b[i]` is a consonant.
    ///
    /// In C, `y` is a consonant at 0 and otherwise the opposite of the
    /// letter before it. Across a run of `y`s that alternates, starting
    /// from the first `y` of the run.
    fn cons(&self, i: isize) -> bool {
        match self.at(i) {
            'a' | 'e' | 'i' | 'o' | 'u' => false,
            'y' => {
                let mut start = i;
                while start > 0 && self.at(start - 1) == 'y' {
                    start -= 1;
                }
                let first_is_consonant = start == 0 || !self.cons(start - 1);
                first_is_consonant == ((i - start) % 2 == 0)
            }
            _ => true,
        }
    }

    /// `m()` measures the number of consonant sequences between 0 and `j`.
    /// If `c` is a consonant sequence and `v` a vowel sequence, and `<..>`
    /// indicates arbitrary presence, `<c><v>` gives 0, `<c>vc<v>` gives 1,
    /// `<c>vcvc<v>` gives 2, and so on.
    fn m(&self) -> usize {
        let mut n = 0;
        let mut i = 0;
        loop {
            if i > self.j {
                return n;
            }
            if !self.cons(i) {
                break;
            }
            i += 1;
        }
        i += 1;
        loop {
            loop {
                if i > self.j {
                    return n;
                }
                if self.cons(i) {
                    break;
                }
                i += 1;
            }
            i += 1;
            n += 1;
            loop {
                if i > self.j {
                    return n;
                }
                if !self.cons(i) {
                    break;
                }
                i += 1;
            }
            i += 1;
        }
    }

    /// `vowelinstem()` is true <=> `0 ..= j` contains a vowel.
    fn vowel_in_stem(&self) -> bool {
        (0..=self.j).any(|i| !self.cons(i))
    }

    /// `doublec(j)` is true <=> `j, j-1` contain a double consonant.
    fn doublec(&self, j: isize) -> bool {
        if j < 1 {
            return false;
        }
        if self.at(j) != self.at(j - 1) {
            return false;
        }
        self.cons(j)
    }

    /// `cvc(i)` is true <=> `i-2, i-1, i` has the form consonant - vowel -
    /// consonant and also if the second c is not w, x or y. This is used
    /// when trying to restore an e at the end of a short word: cav(e),
    /// lov(e), hop(e), crim(e), but snow, box, tray.
    fn cvc(&self, i: isize) -> bool {
        if i < 2 || !self.cons(i) || self.cons(i - 1) || !self.cons(i - 2) {
            return false;
        }
        !matches!(self.at(i), 'w' | 'x' | 'y')
    }

    /// `ends(s)` is true <=> `0 ..= k` ends with the string `s`; it then
    /// sets `j` to the offset just before `s`.
    fn ends(&mut self, s: &str) -> bool {
        let length = s.chars().count() as isize;
        if length > self.k + 1 {
            return false;
        }
        let start = self.k - length + 1;
        if !s.chars().zip(start..).all(|(c, i)| self.at(i) == c) {
            return false;
        }
        self.j = self.k - length;
        true
    }

    /// `setto(s)` sets `j+1 ..= k` to the characters in the string `s`,
    /// readjusting `k`.
    fn setto(&mut self, s: &str) {
        let mut length = 0;
        for (offset, c) in s.chars().enumerate() {
            let at = (self.j + 1) as usize + offset;
            match self.b.get_mut(at) {
                Some(slot) => *slot = c,
                None => self.b.push(c),
            }
            length += 1;
        }
        self.k = self.j + length;
    }

    /// `r(s)` is used further down.
    fn r(&mut self, s: &str) {
        if self.m() > 0 {
            self.setto(s);
        }
    }

    /// `step1ab()` gets rid of plurals and -ed or -ing. e.g. caresses ->
    /// caress, ponies -> poni, ties -> ti, caress -> caress, cats -> cat,
    /// feed -> feed, agreed -> agree, disabled -> disable, matting -> mat,
    /// mating -> mate, meeting -> meet, milling -> mill, messing -> mess,
    /// meetings -> meet.
    fn step1ab(&mut self) {
        if self.at(self.k) == 's' {
            if self.ends("sses") {
                self.k -= 2;
            } else if self.ends("ies") {
                self.setto("i");
            } else if self.at(self.k - 1) != 's' {
                self.k -= 1;
            }
        }
        if self.ends("eed") {
            if self.m() > 0 {
                self.k -= 1;
            }
        } else if (self.ends("ed") || self.ends("ing")) && self.vowel_in_stem() {
            self.k = self.j;
            if self.ends("at") {
                self.setto("ate");
            } else if self.ends("bl") {
                self.setto("ble");
            } else if self.ends("iz") {
                self.setto("ize");
            } else if self.doublec(self.k) {
                self.k -= 1;
                if matches!(self.at(self.k), 'l' | 's' | 'z') {
                    self.k += 1;
                }
            } else if self.m() == 1 && self.cvc(self.k) {
                self.setto("e");
            }
        }
    }

    /// `step1c()` turns terminal y to i when there is another vowel in the
    /// stem.
    fn step1c(&mut self) {
        if self.ends("y") && self.vowel_in_stem() {
            let k = self.k as usize;
            self.b[k] = 'i';
        }
    }

    /// `step2()` maps double suffices to single ones. so -ization ( = -ize
    /// plus -ation) maps to -ize etc. Note that the string before the suffix
    /// must give `m() > 0`.
    fn step2(&mut self) {
        match self.at(self.k - 1) {
            'a' => {
                if self.ends("ational") {
                    self.r("ate");
                } else if self.ends("tional") {
                    self.r("tion");
                }
            }
            'c' => {
                if self.ends("enci") {
                    self.r("ence");
                } else if self.ends("anci") {
                    self.r("ance");
                }
            }
            'e' => {
                if self.ends("izer") {
                    self.r("ize");
                }
            }
            'l' => {
                // -DEPARTURE-: the published algorithm has `abli` -> `able`.
                if self.ends("bli") {
                    self.r("ble");
                } else if self.ends("alli") {
                    self.r("al");
                } else if self.ends("entli") {
                    self.r("ent");
                } else if self.ends("eli") {
                    self.r("e");
                } else if self.ends("ousli") {
                    self.r("ous");
                }
            }
            'o' => {
                if self.ends("ization") {
                    self.r("ize");
                } else if self.ends("ation") || self.ends("ator") {
                    self.r("ate");
                }
            }
            's' => {
                if self.ends("alism") {
                    self.r("al");
                } else if self.ends("iveness") {
                    self.r("ive");
                } else if self.ends("fulness") {
                    self.r("ful");
                } else if self.ends("ousness") {
                    self.r("ous");
                }
            }
            't' => {
                if self.ends("aliti") {
                    self.r("al");
                } else if self.ends("iviti") {
                    self.r("ive");
                } else if self.ends("biliti") {
                    self.r("ble");
                }
            }
            'g' => {
                // -DEPARTURE-: not in the published algorithm.
                if self.ends("logi") {
                    self.r("log");
                }
            }
            _ => {}
        }
    }

    /// `step3()` deals with -ic-, -full, -ness etc. Similar strategy to
    /// step2.
    fn step3(&mut self) {
        match self.at(self.k) {
            'e' => {
                if self.ends("icate") {
                    self.r("ic");
                } else if self.ends("ative") {
                    self.r("");
                } else if self.ends("alize") {
                    self.r("al");
                }
            }
            'i' => {
                if self.ends("iciti") {
                    self.r("ic");
                }
            }
            'l' => {
                if self.ends("ical") {
                    self.r("ic");
                } else if self.ends("ful") {
                    self.r("");
                }
            }
            's' => {
                if self.ends("ness") {
                    self.r("");
                }
            }
            _ => {}
        }
    }

    /// `step4()` takes off -ant, -ence etc., in context `<c>vcvc<v>`.
    fn step4(&mut self) {
        let found = match self.at(self.k - 1) {
            'a' => self.ends("al"),
            'c' => self.ends("ance") || self.ends("ence"),
            'e' => self.ends("er"),
            'i' => self.ends("ic"),
            'l' => self.ends("able") || self.ends("ible"),
            'n' => self.ends("ant") || self.ends("ement") || self.ends("ment") || self.ends("ent"),
            'o' => {
                (self.ends("ion") && self.j >= 0 && matches!(self.at(self.j), 's' | 't'))
                    // takes care of -ous
                    || self.ends("ou")
            }
            's' => self.ends("ism"),
            't' => self.ends("ate") || self.ends("iti"),
            'u' => self.ends("ous"),
            'v' => self.ends("ive"),
            'z' => self.ends("ize"),
            _ => false,
        };
        if found && self.m() > 1 {
            self.k = self.j;
        }
    }

    /// `step5()` removes a final -e if `m() > 1`, and changes -ll to -l if
    /// `m() > 1`.
    fn step5(&mut self) {
        self.j = self.k;
        if self.at(self.k) == 'e' {
            let a = self.m();
            if a > 1 || (a == 1 && !self.cvc(self.k - 1)) {
                self.k -= 1;
            }
        }
        if self.at(self.k) == 'l' && self.doublec(self.k) && self.m() > 1 {
            self.k -= 1;
        }
    }
}
