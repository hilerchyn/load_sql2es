use serde_json::{Value, json};
use std::io;

struct RowItem {
    field_num: i32,
    field_type: String,
    field_content: String,
}

pub struct SQLRow {
    id: i32,
    items: Vec<RowItem>,
}

impl SQLRow {
    pub fn new() -> Self {
        SQLRow {
            items: Vec::new(),
            id: 0,
        }
    }

    pub fn get_id(&mut self) -> i32 {
        self.id
    }

    pub fn append_item(
        &mut self,
        num: i32,
        field_type: String,
        field_content: String,
    ) -> io::Result<()> {
        // println!("sqlrow: {}->{}", num, field_content);

        let item = RowItem {
            field_num: num,
            field_type,
            field_content: field_content.clone(),
        };

        if num == 1 {
            match field_content.parse::<i32>() {
                Ok(number) => {
                    self.id = number;
                }
                Err(e) => {
                    println!("error converting {} to i32: {}", field_content, e);
                }
            }
        }

        self.items.push(item);

        Ok(())
    }

    // 将记录转换为JSON
    #[allow(dead_code)]
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
                7 => field_name = "upload_time",
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

    // 将记录转换为JSON
    pub fn to_jsondoc(&mut self) -> Value {
        let mut record_id: u64 = 0;
        let mut code = "".to_lowercase();
        let mut device_id = "".to_lowercase();
        let mut receive_msg = "".to_lowercase();
        let mut create_time: u64 = 0;
        let mut sys_num = "";
        let mut upload_time = "";
        let mut reason_msg = "".to_lowercase();

        let mut result = String::from("{");
        let mut comma = "";
        for item in &self.items {
            let mut field_name = "record_id";
            match item.field_num {
                1 => {
                    field_name = "record_id";
                    match item.field_content.parse::<u64>() {
                        Ok(num) => record_id = num,
                        Err(e) => println!("Error: {}", e),
                    }
                }
                2 => {
                    field_name = "device_id";
                    device_id = item
                        .field_content
                        .trim_start_matches("'")
                        .trim_end_matches("'")
                        .to_lowercase();
                }
                3 => {
                    field_name = "code";
                    code = item
                        .field_content
                        .trim_start_matches("'")
                        .trim_end_matches("'")
                        .to_lowercase();
                }
                4 => {
                    field_name = "receive_msg";
                    receive_msg = item
                        .field_content
                        .trim_start_matches("'")
                        .trim_end_matches("'")
                        .to_lowercase();
                }
                5 => {
                    field_name = "create_time";
                    match item.field_content.parse::<u64>() {
                        Ok(num) => create_time = num,
                        Err(e) => println!("Error: {}", e),
                    }
                }
                6 => {
                    field_name = "sys_num";
                    sys_num = item.field_content.as_str();
                }
                7 => {
                    field_name = "upload_time";
                    upload_time = item.field_content.as_str();
                }
                8 => {
                    field_name = "reason_msg";
                    reason_msg = item
                        .field_content
                        .trim_start_matches("'")
                        .trim_end_matches("'")
                        .to_lowercase();
                }
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

            // println!("content: {}", content);
            // println!("brace: {}, {}", type_left_brace, type_right_brace);

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

        json!({
            "record_id": record_id,
            "device_id": device_id,
            "code": code,
            "receive_msg": receive_msg,
            "create_time": create_time,
            "sys_num": sys_num,
            "upload_time": upload_time,
            "reason_msg": reason_msg,

        })
    }
}
