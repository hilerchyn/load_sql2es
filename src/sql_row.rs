use std::io;

struct RowItem {
    field_num: i32,
    field_type: String,
    field_content: String,
}

pub struct SQLRow {
    items: Vec<RowItem>,
}

impl SQLRow {
    pub fn new() -> Self {
        SQLRow { items: Vec::new() }
    }

    pub fn append_item(
        &mut self,
        num: i32,
        field_type: String,
        field_content: String,
    ) -> io::Result<()> {
        println!("sqlrow: {}->{}", num, field_content);

        let item = RowItem {
            field_num: num,
            field_type: field_type,
            field_content: field_content,
        };

        self.items.push(item);

        Ok(())
    }

    // 将记录转换为JSON
    pub fn to_json(&mut self) -> String {
        let mut result = String::from("{");
        let mut comma = "";
        for item in &self.items {
            let mut field_name = "record_id";
            match item.field_num {
                1 => {
                    field_name = "record_id";
                }
                2 => {
                    field_name = "device_id";
                }
                3 => field_name = "code",
                4 => field_name = "receive_msg",
                5 => field_name = "create_time",
                6 => field_name = "sys_num",
                7 => field_name = "uploade_time",
                8 => field_name = "reason_msg",
                _ => {
                    eprintln!(
                        "failed to parse record to json: field_num[{}]",
                        item.field_num
                    );
                }
            }

            let mut type_left_brace = "";
            let mut type_right_brace = "";
            let mut content = item.field_content.clone();
            if item.field_type == "str" {
                type_left_brace = "\"";
                type_right_brace = "\"";
                content = content.trim_start_matches("'").to_lowercase();
                content = content.trim_end_matches("'").to_lowercase();
            }

            if item.field_content == "NULL" {
                content = String::from("null");
            }

            result.push_str(
                format!(
                    "{}\"{}\":{}{}{}",
                    comma, field_name, type_left_brace, content, type_right_brace
                )
                .as_str(),
            );
            comma = ",";
        }

        result.push_str("}");

        result
    }
}
