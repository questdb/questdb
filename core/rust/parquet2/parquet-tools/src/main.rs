use parquet_tools::dump::dump_file;
use parquet_tools::meta::show_meta;
use parquet_tools::rows::show_rows;

use parquet_tools::Result;

use clap::{load_yaml, App};

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    let file_name = match matches.value_of("file") {
        Some(file_name) => file_name,
        None => {
            eprintln!("No parquet file to read");
            std::process::exit(1);
        }
    };

    let mut output = std::io::stdout();
    if matches.subcommand_matches("rowcount").is_some() {
        show_rows(file_name, &mut output)?;
    }

    if let Some(matches) = matches.subcommand_matches("meta") {
        show_meta(
            file_name,
            matches.is_present("extra"),
            matches.is_present("stats"),
            &mut output,
        )?;
    }

    if let Some(matches) = matches.subcommand_matches("dump") {
        // The columns to be listed can be selected using the columns argument
        // If no argument is chosen then a sample of all the columns if presented
        let columns: Option<Vec<usize>> = match matches.values_of("columns") {
            Some(values) => {
                let columns: std::result::Result<Vec<usize>, _> =
                    values.map(|val| val.parse::<usize>()).collect();
                Some(columns?)
            }
            None => None,
        };

        dump_file(file_name, columns, &mut output)?;
    }

    Ok(())
}
