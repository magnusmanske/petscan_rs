use crate::datasource::SQLtuple;
use crate::platform::Platform;
use mysql_async::Value as MyValue;

impl Platform {
    fn get_label_sql_helper(&self, ret: &mut SQLtuple, part1: &str, part2: &str) {
        let mut types: Vec<String> = vec![];
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_l")) {
            types.push("1".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_a")) {
            types.push("3".to_string());
        }
        if self.has_param(&("cb_labels_".to_owned() + part1 + "_d")) {
            types.push("2".to_string());
        }
        if !types.is_empty() {
            let mut tmp = Self::prep_quote(&types);
            ret.0 += &(" AND ".to_owned() + part2 + " IN (" + &tmp.0 + ")");
            ret.1.append(&mut tmp.1);
        }
    }

    pub fn get_label_sql(&self) -> SQLtuple {
        let mut ret: SQLtuple = (String::new(), vec![]);
        let yes = self.get_param_as_vec("labels_yes", "\n");
        let any = self.get_param_as_vec("labels_any", "\n");
        let no = self.get_param_as_vec("labels_no", "\n");
        if yes.len() + any.len() + no.len() == 0 {
            return ret;
        }

        let langs_yes = self.get_param_as_vec("langs_labels_yes", ",");
        let langs_any = self.get_param_as_vec("langs_labels_any", ",");
        let langs_no = self.get_param_as_vec("langs_labels_no", ",");

        ret.0 = "SELECT DISTINCT concat('Q',wbit_item_id) AS term_full_entity_id
            FROM wbt_text,wbt_item_terms wbt_item_terms1,wbt_term_in_lang,wbt_text_in_lang
            WHERE wbit_term_in_lang_id = wbtl_id
            AND wbtl_text_in_lang_id = wbxl_id
            AND wbxl_text_id = wbx_id"
            .to_string();

        yes.iter().for_each(|s| {
            if s != "%" {
                ret.0 += " AND wbx_text LIKE ?";
                ret.1.push(MyValue::Bytes(s.to_owned().into()));
            }
            if !langs_yes.is_empty() {
                let mut tmp = Self::prep_quote(&langs_yes);
                ret.0 += &(" AND wbxl_language IN (".to_owned() + &tmp.0 + ")");
                ret.1.append(&mut tmp.1);
                self.get_label_sql_helper(&mut ret, "yes", "wbtl_type_id");
            }
        });

        if !langs_any.is_empty() {
            ret.0 += " AND (";
            let mut first = true;
            any.iter().for_each(|s| {
                if first {
                    first = false;
                } else {
                    ret.0 += " OR ";
                }
                if s != "%" {
                    ret.0 += " ( wbx_text LIKE ?";
                    ret.1.push(MyValue::Bytes(s.to_owned().into()));
                }
                if !langs_any.is_empty() {
                    let mut tmp = Self::prep_quote(&langs_any);
                    ret.0 += &(" AND wbxl_language IN (".to_owned() + &tmp.0 + ")");
                    ret.1.append(&mut tmp.1);
                    self.get_label_sql_helper(&mut ret, "any", "wbtl_type_id");
                }
                ret.0 += ")";
            });
            ret.0 += ")";
        }

        no.iter().for_each(|s| {
            ret.0 += " AND NOT EXISTS (
                SELECT * FROM
                wbt_text wbt_text2,
                wbt_item_terms wbt_item_terms2,
                wbt_term_in_lang wbt_term_in_lang2,
                wbt_text_in_lang wbt_text_in_lang2
                WHERE wbt_item_terms2.wbit_term_in_lang_id = wbt_term_in_lang2.wbtl_id
                AND wbt_term_in_lang2.wbtl_text_in_lang_id = wbt_text_in_lang2.wbxl_id
                AND wbt_text_in_lang2.wbxl_text_id = wbt_text2.wbx_id
                AND wbt_item_terms1.wbit_item_id=wbt_item_terms2.wbit_item_id";
            if s != "%" {
                ret.0 += " AND wbt_text2.wbx_text LIKE ?";
                ret.1.push(MyValue::Bytes(s.to_owned().into()));
            }
            if !langs_no.is_empty() {
                let mut tmp = Self::prep_quote(&langs_no);
                ret.0 += &(" AND wbt_type2.wbxl_language IN (".to_owned() + &tmp.0 + ")");
                ret.1.append(&mut tmp.1);
                self.get_label_sql_helper(&mut ret, "no", "wbt_term_in_lang2.wbtl_type_id");
            }
            ret.0 += ")";
        });
        ret
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

    #[test]
    fn test_get_label_sql_empty_when_no_labels() {
        let p = make_platform(vec![]);
        let sql = p.get_label_sql();
        assert!(sql.0.is_empty());
        assert!(sql.1.is_empty());
    }

    #[test]
    fn test_get_label_sql_yes_label() {
        let p = make_platform(vec![("labels_yes", "foo")]);
        let sql = p.get_label_sql();
        assert!(!sql.0.is_empty());
        assert!(sql.0.contains("wbx_text LIKE ?"));
        // One placeholder for the label text
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn test_get_label_sql_no_label() {
        let p = make_platform(vec![("labels_no", "bar")]);
        let sql = p.get_label_sql();
        assert!(!sql.0.is_empty());
        assert!(sql.0.contains("NOT EXISTS"));
        assert_eq!(sql.1.len(), 1);
    }

    #[test]
    fn test_get_label_sql_wildcard_skips_text_bind() {
        // When the label is "%" no LIKE binding should be added
        let p = make_platform(vec![("labels_yes", "%")]);
        let sql = p.get_label_sql();
        // The SELECT base is generated but no bound parameter for "%"
        assert!(!sql.0.is_empty());
        assert!(sql.1.is_empty());
    }

    #[test]
    fn test_get_label_sql_yes_with_language() {
        let p = make_platform(vec![("labels_yes", "foo"), ("langs_labels_yes", "en,de")]);
        let sql = p.get_label_sql();
        assert!(sql.0.contains("wbxl_language IN"));
        // 1 for the label + 2 for the languages
        assert_eq!(sql.1.len(), 3);
    }

    #[test]
    fn test_get_label_helper_appends_type_constraint() {
        // cb_labels_yes_l → type 1 (label)
        let p = make_platform(vec![
            ("labels_yes", "test"),
            ("cb_labels_yes_l", "1"),
            ("langs_labels_yes", "en"),
        ]);
        let sql = p.get_label_sql();
        assert!(sql.0.contains("wbtl_type_id"));
    }
}
