use clap::Parser;
use elasticsearch::{BulkOperation, BulkParts};
use serde_json::Value;
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

mod sql_row;
use sql_row::SQLRow;

mod esclient;
use esclient::EsClient;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(short, long)]
    file: String,

    #[arg(short, long, default_value_t = String::from("rts_iot_upload_data_aliyun"))]
    index_name: String,

    #[arg(short, long, default_value_t = String::from("https://127.0.0.1:9200"))]
    es_host: String,

    #[arg(short, long, default_value_t = String::from("elastic"))]
    es_usename: String,

    #[arg(short, long, default_value_t = String::from("GrjXqOPYXAO7gPxlv4P2"))]
    es_password: String,

    #[arg(short = 'd', long, default_value_t = false)]
    debug: bool,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli_args = Args::parse();

    // 设置默认解析文件
    let file_name: String = cli_args.file;
    let index_name: String = cli_args.index_name;

    let mut es: EsClient = EsClient::new(
        &cli_args.es_host,
        &cli_args.es_usename,
        &cli_args.es_password,
    );
    let es_mut_ref = &mut es;

    // 打开文件
    let file = File::open(file_name)?;
    // 定义文件读取器
    let reader = BufReader::new(file);

    // 读取指定行数后推出标记变量
    let mut count = 0;
    // 逐行遍历数据文件
    for line in reader.lines() {
        let temp_line = &line;
        match temp_line {
            Ok(s) => {
                // 解析插入SQL语句
                //let _ = parse_insert_sql(es_mut_ref, s);
                // let ok: bool = parse_insert_sql(es, s);
                // if !ok {
                //     continue;
                // }
                match parse_insert_sql(es_mut_ref, s, index_name.as_str()).await {
                    true => println!("submmitted"),
                    false => eprintln!("failed"),
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }

        // 推出行数标记变量
        count = count + 1;
        //println!("[{}]:\t{}", count, line?);
        if count == 50 {
            //break;
        }
    }

    Ok(())
}

// 解析插入SQL语句
async fn parse_insert_sql(es_client: &mut EsClient, sql: &String, index_name: &str) -> bool {
    // 解析包含 VALUES 的文本
    let ps: Vec<String> = sql.split("VALUES (").map(String::from).collect();
    if ps.len() != 2 {
        return false;
    }

    let mut operations: Vec<BulkOperation<Value>> = Vec::new();

    // 获取 INSERT 语句中，批量写入的数据。
    // 并拆分为数组
    let parts: Vec<String> = (&ps[1]).split("),(").map(String::from).collect();
    // println!("lenght: {}", parts.len());
    for part in parts {
        let record: &str = part.trim_end_matches(");");
        // println!("part: {}", record);

        let s: String = String::from(record);
        let mut comma_opened = false;
        let mut field = String::from("");
        let mut field_num = 1;
        let mut field_type = String::from("int");
        let mut record = SQLRow::new();
        for c in s.chars() {
            // 用引号包括的字段，不需要分解
            if c == '\'' {
                comma_opened = !comma_opened;

                // 有单引号则使用字符串类型
                if comma_opened {
                    field_type = String::from("str");
                }
            }

            // 字段结束
            if c == ',' && !comma_opened {
                // println!("field: [{}]: {}", field_num, field);
                let _ = record.append_item(field_num, field_type, field.clone());
                field_num = field_num + 1;
                field = String::from("");
                field_type = String::from("int");
                continue;
            }
            field.push(c);
        }
        let _ = record.append_item(field_num, field_type, field.clone());
        // println!("field: [{}]: {}", field_num, field);
        // println!("json: {}", record.to_json());

        // 插入数据到ES
        operations.push(
            BulkOperation::index(record.to_jsondoc())
                .id(format!("{}", record.get_id()))
                .into(),
        );
    }

    let index = index_name;
    let client = es_client.get_client();
    let bulk_response = match client
        .bulk(BulkParts::Index(index))
        .body(operations)
        //.header(
        //    HeaderName::from_static("Content-Type"),
        //    HeaderValue::from_static("application/json"),
        //)
        .send()
        .await
    {
        Ok(response) => response,
        Err(e) => {
            eprintln!("failed to send bulk request: {}", e);
            return false;
        }
    };

    let response_body = match bulk_response.json::<Value>().await {
        Ok(body) => body,
        Err(e) => {
            eprintln!("Failed to parse bulk response: {}", e);
            return false;
        }
    };

    match response_body["errors"].as_bool() {
        Some(false) => {
            // println!("Bulk indexed {} records to '{}'", record.get_id(), index);
            true
        }
        _ => {
            eprintln!("Bulk indexing errors: {:?}", response_body["items"]);
            false
        }
    };

    true
}
