#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;
extern crate pest;
#[macro_use]
extern crate pest_derive;
#[cfg(test)]
#[macro_use]
extern crate lazy_static;
mod analyzer;
mod parser;
mod processor;
mod task;

use log::{error, trace};
use std::error::Error as StdError;
use std::ffi::OsStr;
use std::io::{Error as IoError, ErrorKind};
use std::path::Path;
use std::{env, fs, process};

fn main() {
    env_logger::init();
    let args = env::args().collect::<Vec<_>>();
    validate_arg_count(args.len());
    let file_path = &args[1];
    trace!("reading file from path...");
    match fs::read_to_string(file_path) {
        Ok(unparsed_file_content) => match processor::process(&unparsed_file_content) {
            Ok(analysis) => {
                trace!("rendering analysis...");
                println!("{}", analysis);
            }
            Err(err) => {
                trace!("ending with a processing error...");
                handle_processing_error(err);
            }
        },
        Err(err) => {
            trace!("ending with an I/O error...");
            let program_name = get_executable_name(&args[0]).unwrap_or(&args[0]);
            handle_io_error(err, program_name, file_path);
        }
    }
}

fn get_executable_name(exec_path: &str) -> Option<&str> {
    Path::new(exec_path).file_name().and_then(OsStr::to_str)
}

fn validate_arg_count(arg_count: usize) {
    let expected_arg_count = 2usize;
    if arg_count != expected_arg_count {
        let usage_message = "usage: ./analyze-task-schedule file";
        eprintln!("{}", usage_message);
        process::exit(1);
    }
}

fn handle_processing_error<'a>(err: Box<dyn StdError + 'a>) {
    error!("Error: {}", err);
    eprintln!("Error: {}", err);
    process::exit(1);
}

fn handle_io_error(err: IoError, program_name: &str, file_path: &str) {
    let mut err_str = String::new();
    match err.kind() {
        ErrorKind::NotFound => {
            err_str.push_str(&format!("{}: {}: No such file", program_name, file_path));
        }
        ErrorKind::PermissionDenied => {
            err_str.push_str(&format!(
                "{}: {}: Access to file is denied",
                program_name, file_path
            ));
        }
        _ => {
            err_str.push_str(&format!(
                "{}: {}: Encountered an error while opening the file: {}",
                program_name, file_path, err
            ));
        }
    }
    error!("{}", err_str);
    eprintln!("{}", err_str);
    process::exit(1);
}
