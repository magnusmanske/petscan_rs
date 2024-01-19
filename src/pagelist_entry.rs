use serde_json::Value;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use wikibase::mediawiki::api::NamespaceID;
use wikibase::mediawiki::title::Title;

//________________________________________________________________________________________________________________________


#[derive(Debug, Clone, PartialEq)]
pub enum PageListSort {
    Default(bool),
    Title(bool),
    NsTitle(bool),
    Size(bool),
    Date(bool),
    RedlinksCount(bool),
    IncomingLinks(bool),
    DefaultSort(bool),
    FileSize(bool),
    UploadDate(bool),
    Sitelinks(bool),
    Random(bool),
}

impl PageListSort {
    pub fn new_from_params(s: &str, descending: bool) -> Self {
        match s {
            "title" => Self::Title(descending),
            "ns_title" => Self::NsTitle(descending),
            "size" => Self::Size(descending),
            "date" => Self::Date(descending),
            "redlinks" => Self::RedlinksCount(descending),
            "incoming_links" => Self::IncomingLinks(descending),
            "defaultsort" => Self::DefaultSort(descending),
            "filesize" => Self::FileSize(descending),
            "uploaddate" => Self::UploadDate(descending),
            "sitelinks" => Self::Sitelinks(descending),
            "random" => Self::Random(descending),
            _ => Self::Default(descending),
        }
    }
}

//________________________________________________________________________________________________________________________


#[derive(Debug, Clone, PartialEq, Default)]
pub struct FileInfo {
    pub file_usage: Vec<FileUsage>,
    pub img_size: Option<usize>,
    pub img_width: Option<usize>,
    pub img_height: Option<usize>,
    pub img_media_type: Option<String>,
    pub img_major_mime: Option<String>,
    pub img_minor_mime: Option<String>,
    pub img_user_text: Option<String>,
    pub img_timestamp: Option<String>,
    pub img_sha1: Option<String>,
}

impl FileInfo {
    pub fn new_from_gil_group(gil_group: &str) -> Self {
        let mut ret = Self::new();
        ret.file_usage = gil_group
            .split('|')
            .filter_map(|part| FileUsage::new_from_part(&part.to_string()))
            .collect();
        ret
    }

    pub fn new() -> Self {
        Self { ..Default::default() }
    }
}

//________________________________________________________________________________________________________________________


#[derive(Debug, Clone, PartialEq)]
pub struct FileUsage {
    title: Title,
    wiki: String,
    namespace_name: String,
}

impl FileUsage {
    pub fn new_from_part(part: &str) -> Option<Self> {
        let mut parts = part.split(':');
        let wiki = parts.next()?;
        let namespace_id = parts.next()?.parse::<NamespaceID>().ok()?;
        let namespace_name = parts.next()?;
        let page = parts.collect::<Vec<&str>>().join(":");
        if page.is_empty() {
            return None;
        }
        Some(Self {
            title: Title::new(&page, namespace_id),
            namespace_name: namespace_name.to_string(),
            wiki: wiki.to_string(),
        })
    }

    pub fn wiki(&self) -> &String {
        &self.wiki
    }

    pub fn title(&self) -> &Title {
        &self.title
    }

    pub fn namespace_name(&self) -> &String {
        &self.namespace_name
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct PageCoordinates {
    pub lat: f64,
    pub lon: f64,
}

impl PageCoordinates {
    pub fn new_from_lat_lon(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split(',').collect();
        let lat = parts.get(0)?.parse::<f64>().ok()?;
        let lon = parts.get(1)?.parse::<f64>().ok()?;
        Some(Self { lat, lon })
    }
}

//________________________________________________________________________________________________________________________


pub type LinkCount = u32;

#[derive(Debug, Clone, PartialEq)]
pub enum TriState {
    Yes,
    No,
    Unknown,
}

impl TriState {
    pub fn as_json(&self) -> Value {
        match self {
            Self::Yes => json!(true),
            Self::No => json!(false),
            Self::Unknown => Value::Null,
        }
    }

    pub fn as_option_bool(&self) -> Option<bool> {
        match self {
            Self::Yes => Some(true),
            Self::No => Some(false),
            Self::Unknown => None,
        }
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone)]
pub struct PageListEntry {
    title: Title,
    pub disambiguation: TriState,
    pub page_id: Option<u32>,
    pub page_bytes: Option<u32>,
    pub incoming_links: Option<LinkCount>,
    pub link_count: Option<LinkCount>,
    pub redlink_count: Option<LinkCount>,
    pub sitelink_count: Option<LinkCount>,
    page_timestamp: Option<Box<String>>,
    page_image: Option<Box<String>>,
    wikidata_item: Option<Box<String>>,
    wikidata_label: Option<Box<String>>,
    wikidata_description: Option<Box<String>>,
    defaultsort: Option<Box<String>>,
    coordinates: Option<Box<PageCoordinates>>,
    file_info: Option<Box<FileInfo>>,
}

impl Hash for PageListEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.title.namespace_id().hash(state);
        self.title.pretty().hash(state);
    }
}

impl PartialEq for PageListEntry {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title
    }
}

impl Eq for PageListEntry {}

impl PageListEntry {
    pub fn new(title: Title) -> Self {
        Self {
            title,
            wikidata_item: None,
            page_id: None,
            page_bytes: None,
            page_timestamp: None,
            defaultsort: None,
            disambiguation: TriState::Unknown,
            incoming_links: None,
            page_image: None,
            coordinates: None,
            link_count: None,
            sitelink_count: None,
            file_info: None,
            wikidata_label: None,
            wikidata_description: None,
            redlink_count: None,
        }
    }

    pub fn get_file_info(&self) -> Option<FileInfo> {
        match &self.file_info {
            Some(file_info) => Some(*(file_info.clone())),
            None => None,
        }
    }

    pub fn set_file_info(&mut self, file_info_option: Option<FileInfo>) {
        self.file_info = match file_info_option {
            Some(file_info) => Some(Box::new(file_info)),
            None => None,
        }
    }

    pub fn get_coordinates(&self) -> Option<PageCoordinates> {
        match &self.coordinates {
            Some(coordinates) => Some(*(coordinates.clone())),
            None => None,
        }
    }

    pub fn set_coordinates(&mut self, coordinates_option: Option<PageCoordinates>) {
        self.coordinates = match coordinates_option {
            Some(coordinates) => Some(Box::new(coordinates)),
            None => None,
        }
    }

    pub fn get_defaultsort(&self) -> Option<String> {
        match &self.defaultsort {
            Some(defaultsort) => Some(*(defaultsort.clone())),
            None => None,
        }
    }

    pub fn set_defaultsort(&mut self, defaultsort_option: Option<String>) {
        self.defaultsort = match defaultsort_option {
            Some(defaultsort) => Some(Box::new(defaultsort)),
            None => None,
        }
    }

    pub fn get_wikidata_description(&self) -> Option<String> {
        match &self.wikidata_description {
            Some(wikidata_description) => Some(*(wikidata_description.clone())),
            None => None,
        }
    }

    pub fn set_wikidata_description(&mut self, wikidata_description_option: Option<String>) {
        self.wikidata_description = match wikidata_description_option {
            Some(wikidata_description) => Some(Box::new(wikidata_description)),
            None => None,
        }
    }

    pub fn get_wikidata_label(&self) -> Option<String> {
        match &self.wikidata_label {
            Some(wikidata_label) => Some(*(wikidata_label.clone())),
            None => None,
        }
    }

    pub fn set_wikidata_label(&mut self, wikidata_label_option: Option<String>) {
        self.wikidata_label = match wikidata_label_option {
            Some(wikidata_label) => Some(Box::new(wikidata_label)),
            None => None,
        }
    }

    pub fn get_wikidata_item(&self) -> Option<String> {
        match &self.wikidata_item {
            Some(wikidata_item) => Some(*(wikidata_item.clone())),
            None => None,
        }
    }

    pub fn set_wikidata_item(&mut self, wikidata_item_option: Option<String>) {
        self.wikidata_item = match wikidata_item_option {
            Some(wikidata_item) => Some(Box::new(wikidata_item)),
            None => None,
        }
    }

    pub fn get_page_image(&self) -> Option<String> {
        match &self.page_image {
            Some(page_image) => Some(*(page_image.clone())),
            None => None,
        }
    }

    pub fn set_page_image(&mut self, page_image_option: Option<String>) {
        self.page_image = match page_image_option {
            Some(page_image) => Some(Box::new(page_image)),
            None => None,
        }
    }

    pub fn get_page_timestamp(&self) -> Option<String> {
        match &self.page_timestamp {
            Some(page_timestamp) => Some(*(page_timestamp.clone())),
            None => None,
        }
    }

    pub fn set_page_timestamp(&mut self, page_timestamp_option: Option<String>) {
        self.page_timestamp = match page_timestamp_option {
            Some(page_timestamp) => Some(Box::new(page_timestamp)),
            None => None,
        }
    }

    pub fn title(&self) -> &Title {
        &self.title
    }

    pub fn compare(&self, other: &Self, sorter: &PageListSort, is_wikidata: bool) -> Ordering {
        match sorter {
            PageListSort::Default(d) => self.compare_by_page_id(other, *d),
            PageListSort::Title(d) => {
                if is_wikidata {
                    self.compare_by_label(other, *d)
                } else {
                    self.compare_by_title(other, *d)
                }
            }
            PageListSort::NsTitle(d) => self.compare_by_ns_title(other, *d),
            PageListSort::Size(d) => self.compare_by_size(other, *d),
            PageListSort::IncomingLinks(d) => self.compare_by_incoming(other, *d),
            PageListSort::DefaultSort(d) => self.compare_by_defaultsort(other, *d, is_wikidata),
            PageListSort::Date(d) => self.compare_by_date(other, *d),
            PageListSort::UploadDate(d) => self.compare_by_upload_date(other, *d),
            PageListSort::FileSize(d) => self.compare_by_file_size(other, *d),
            PageListSort::RedlinksCount(d) => self.compare_by_redlinks(other, *d),
            PageListSort::Sitelinks(d) => self.compare_by_sitelinks(other, *d),
            PageListSort::Random(d) => self.compare_by_random(other, *d),
        }
    }

    fn compare_by_page_id(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.page_id, &other.page_id, descending)
    }

    fn compare_by_redlinks(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.redlink_count, &other.redlink_count, descending)
    }

    fn compare_by_random(
        self: &PageListEntry,
        _other: &PageListEntry,
        _descending: bool,
    ) -> Ordering {
        if rand::random() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }

    fn compare_by_size(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        self.compare_by_opt(&self.page_bytes, &other.page_bytes, descending)
    }

    fn compare_by_incoming(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.incoming_links, &other.incoming_links, descending)
    }

    fn get_defaultsort_with_fallback(&self, is_wikidata: bool) -> Option<String> {
        match &self.defaultsort {
            Some(x) => Some(x.to_string()),
            None => {
                if is_wikidata {
                    self.get_wikidata_label()
                } else {
                    Some(self.title.pretty().to_owned())
                }
            }
        }
    }

    fn compare_by_defaultsort(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
        is_wikidata: bool,
    ) -> Ordering {
        let ds_mine = self.get_defaultsort_with_fallback(is_wikidata) ;
        let ds_other = other.get_defaultsort_with_fallback(is_wikidata) ;
        self.compare_by_opt(&ds_mine, &ds_other, descending)
    }

    fn compare_by_sitelinks(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        self.compare_by_opt(&self.sitelink_count, &other.sitelink_count, descending)
    }

    fn compare_by_date(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        self.compare_by_opt(
            &self.get_page_timestamp(),
            &other.get_page_timestamp(),
            descending,
        )
    }

    fn compare_by_file_size(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        match (&self.get_file_info(), &other.get_file_info()) {
            (Some(f1), Some(f2)) => self.compare_by_opt(&f1.img_size, &f2.img_size, descending),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    fn compare_by_upload_date(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        match (&self.get_file_info(), &other.get_file_info()) {
            (Some(f1), Some(f2)) => {
                self.compare_by_opt(&f1.img_timestamp, &f2.img_timestamp, descending)
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    fn compare_by_opt<T: PartialOrd>(
        &self,
        mine: &Option<T>,
        other: &Option<T>,
        descending: bool,
    ) -> Ordering {
        self.compare_order(
            match (mine, other) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Less),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            descending,
        )
    }

    fn compare_by_ns_title(
        self: &PageListEntry,
        other: &PageListEntry,
        descending: bool,
    ) -> Ordering {
        if self.title.namespace_id() == other.title.namespace_id() {
            self.compare_by_title(other, descending)
        } else {
            self.compare_order(
                self.title
                    .namespace_id()
                    .partial_cmp(&other.title.namespace_id())
                    .unwrap_or(Ordering::Less),
                descending,
            )
        }
    }

    fn compare_by_label(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        let l1 = self
            .get_wikidata_label()
            .or_else(|| Some(self.title.pretty().to_owned()))
            .unwrap_or_default()
            .to_lowercase();
        let l2 = other
            .get_wikidata_label()
            .or_else(|| Some(self.title.pretty().to_owned()))
            .unwrap_or_default()
            .to_lowercase();
        self.compare_order(l1.partial_cmp(&l2).unwrap_or(Ordering::Less), descending)
    }

    fn compare_by_title(self: &PageListEntry, other: &PageListEntry, descending: bool) -> Ordering {
        self.compare_order(
            self.title
                .pretty()
                .partial_cmp(other.title.pretty())
                .unwrap_or(Ordering::Less),
            descending,
        )
    }

    fn compare_order(&self, ret: Ordering, descending: bool) -> Ordering {
        if descending {
            ret.reverse()
        } else {
            ret
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn file_usage() {
        // 3 instead of 4 parts
        assert_eq!(
            FileUsage::new_from_part(&"the_wiki:7:the_namespace_name".to_string()),
            None
        );
        // String instead of namespace ID
        assert_eq!(
            FileUsage::new_from_part(
                &"the_wiki:the_namespace_id:the_namespace_name:The:page".to_string()
            ),
            None
        );
        // This should work
        let fu = FileUsage::new_from_part(&"the_wiki:7:the_namespace_name:The:page".to_string())
            .unwrap();
        assert_eq!(fu.wiki(), "the_wiki");
        assert_eq!(fu.namespace_name(), "the_namespace_name");
        assert_eq!(*fu.title(), Title::new("The:page", 7));
    }

    #[test]
    fn file_info() {
        let fu = FileUsage::new_from_part(&"the_wiki:7:the_namespace_name:The:page".to_string())
            .unwrap();
        let fi = FileInfo::new_from_gil_group(
            &"|somesuch|the_wiki:7:the_namespace_name:The:page|the_wiki:7:the_namespace_name"
                .to_string(),
        );
        assert_eq!(fi.file_usage, vec![fu]);
    }

    #[test]
    fn lat_lon() {
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"something".to_string()),
            None
        );
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"-0.1234".to_string()),
            None
        );
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"-0.1234,A".to_string()),
            None
        );
        assert_eq!(
            PageCoordinates::new_from_lat_lon(&"-0.1234,2.345".to_string()),
            Some(PageCoordinates {
                lat: -0.1234,
                lon: 2.345
            })
        );
    }

}