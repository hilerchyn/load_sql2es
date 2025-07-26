use std::fs::File;
use std::io::{self, BufRead, BufReader};

fn main() -> io::Result<()> {
    let file = File::open("./example.sql")?;
    let reader = BufReader::new(file);

    let mut count = 0;
    for line in reader.lines() {
        count = count + 1;
        println!("[{}]:\t{}", count, line?);

        if count == 10 {
            break;
        }
    }

    Ok(())
}
