#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ContentType {
    HTML,
    Plain,
    JSON,
    JSONP,
    CSV,
    TSV,
    KML,
}

impl ContentType {
    pub const fn as_str(&self) -> &str {
        match self {
            Self::HTML => "text/html; charset=utf-8",
            Self::Plain => "text/plain; charset=utf-8",
            Self::JSON => " application/json",
            Self::JSONP => "application/javascript",
            Self::CSV => "text/csv; charset=utf-8",
            Self::TSV => "text/tab-separated-values; charset=utf-8",
            Self::KML => "application/vnd.google-earth.kml+xml",
        }
    }
}
