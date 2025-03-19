use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Combination {
    None,
    Source(String),
    Intersection((Box<Combination>, Box<Combination>)),
    Union((Box<Combination>, Box<Combination>)),
    Not((Box<Combination>, Box<Combination>)),
}

impl fmt::Display for Combination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Combination::None => write!(f, "nothing"),
            Combination::Source(s) => write!(f, "{}", s),
            Combination::Intersection((a, b)) => write!(f, "({} AND {})", a, b),
            Combination::Union((a, b)) => write!(f, "({} OR {})", a, b),
            Combination::Not((a, b)) => write!(f, "({} NOT {})", a, b),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CombinationSequential {
    Source(String),
    Intersection,
    Union,
    Not,
}
