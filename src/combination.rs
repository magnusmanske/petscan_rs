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
            Combination::Source(s) => write!(f, "{s}"),
            Combination::Intersection((a, b)) => write!(f, "({a} AND {b})"),
            Combination::Union((a, b)) => write!(f, "({a} OR {b})"),
            Combination::Not((a, b)) => write!(f, "({a} NOT {b})"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combination_display_none() {
        assert_eq!(format!("{}", Combination::None), "nothing");
    }

    #[test]
    fn test_combination_display_source() {
        assert_eq!(
            format!("{}", Combination::Source("categories".to_string())),
            "categories"
        );
    }

    #[test]
    fn test_combination_display_intersection() {
        let c = Combination::Intersection((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        assert_eq!(format!("{c}"), "(a AND b)");
    }

    #[test]
    fn test_combination_display_union() {
        let c = Combination::Union((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        assert_eq!(format!("{c}"), "(a OR b)");
    }

    #[test]
    fn test_combination_display_not() {
        let c = Combination::Not((
            Box::new(Combination::Source("a".to_string())),
            Box::new(Combination::Source("b".to_string())),
        ));
        assert_eq!(format!("{c}"), "(a NOT b)");
    }

    #[test]
    fn test_combination_equality() {
        let a = Combination::Source("x".to_string());
        let b = Combination::Source("x".to_string());
        assert_eq!(a, b);

        let c = Combination::Source("y".to_string());
        assert_ne!(a, c);
    }

    #[test]
    fn test_combination_nested() {
        let inner = Combination::Union((
            Box::new(Combination::Source("sparql".to_string())),
            Box::new(Combination::Source("pagepile".to_string())),
        ));
        let outer = Combination::Not((
            Box::new(Combination::Source("categories".to_string())),
            Box::new(inner),
        ));
        assert_eq!(format!("{outer}"), "(categories NOT (sparql OR pagepile))");
    }

    #[test]
    fn test_combination_sequential_variants() {
        let src = CombinationSequential::Source("test".to_string());
        assert_eq!(src, CombinationSequential::Source("test".to_string()));
        assert_ne!(src, CombinationSequential::Intersection);
        assert_ne!(CombinationSequential::Union, CombinationSequential::Not);
    }
}
