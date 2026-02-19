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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_type_as_str() {
        assert_eq!(ContentType::HTML.as_str(), "text/html; charset=utf-8");
        assert_eq!(ContentType::Plain.as_str(), "text/plain; charset=utf-8");
        assert_eq!(ContentType::JSON.as_str(), " application/json");
        assert_eq!(ContentType::JSONP.as_str(), "application/javascript");
        assert_eq!(ContentType::CSV.as_str(), "text/csv; charset=utf-8");
        assert_eq!(
            ContentType::TSV.as_str(),
            "text/tab-separated-values; charset=utf-8"
        );
        assert_eq!(
            ContentType::KML.as_str(),
            "application/vnd.google-earth.kml+xml"
        );
    }

    #[test]
    fn test_content_type_equality() {
        assert_eq!(ContentType::HTML, ContentType::HTML);
        assert_ne!(ContentType::HTML, ContentType::JSON);
        assert_ne!(ContentType::CSV, ContentType::TSV);
    }

    #[test]
    fn test_content_type_copy() {
        let ct = ContentType::JSON;
        let ct2 = ct;
        assert_eq!(ct, ct2);
    }
}
