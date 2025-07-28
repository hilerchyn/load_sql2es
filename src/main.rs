use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

mod sql_row;
use sql_row::SQLRow;

fn main() -> io::Result<()> {
    // 设置默认解析文件
    let mut file_name: String = String::from("./example.sql");

    // 从命令行参数中获取指定解析的数据文件
    let args: Vec<String> = env::args().collect();
    for (i, arg) in args.iter().enumerate().skip(1) {
        println!("Arg{}: {}", i, arg);
        // TODO: 替换lowercase函数
        file_name = arg.to_lowercase();
    }

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
                let ok: bool = parse_insert_sql(s);
                if !ok {
                    continue;
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }

        // 推出行数标记变量
        count = count + 1;
        //println!("[{}]:\t{}", count, line?);
        if count == 50 {
            break;
        }
    }

    Ok(())
}

// 解析插入SQL语句
fn parse_insert_sql(sql: &String) -> bool {
    // 解析包含 VALUES 的文本
    let ps: Vec<String> = sql.split("VALUES (").map(String::from).collect();
    if ps.len() != 2 {
        return false;
    }

    // 获取 INSERT 语句中，批量写入的数据。
    // 并拆分为数组
    let parts: Vec<String> = (&ps[1]).split("),(").map(String::from).collect();
    println!("lenght: {}", parts.len());
    for part in parts {
        let record: &str = part.trim_end_matches(");");
        println!("part: {}", record);

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
                println!("field: [{}]: {}", field_num, field);
                let _ = record.append_item(field_num, field_type, field.clone());
                field_num = field_num + 1;
                field = String::from("");
                field_type = String::from("int");
                continue;
            }
            field.push(c);
        }
        let _ = record.append_item(field_num, field_type, field.clone());
        println!("field: [{}]: {}", field_num, field);
        println!("json: {}", record.to_json());
    }

    true
}
