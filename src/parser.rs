use crate::task::{Duration, TaskLabel};
use log::debug;
use pest::error::Error as PestError;
use pest::error::LineColLocation;
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use std::error::Error as StdError;
use std::fmt;

#[derive(Debug)]
pub struct ParsedData<'a> {
    task_orders: Vec<(TaskLabel<'a>, Option<TaskLabel<'a>>)>,
    task_durations: Vec<(TaskLabel<'a>, Duration)>,
}

impl<'a> ParsedData<'a> {
    pub fn task_durations(&self) -> &[(TaskLabel<'a>, Duration)] {
        &self.task_durations
    }

    pub fn task_orders(&self) -> &[(TaskLabel<'a>, Option<TaskLabel<'a>>)] {
        &self.task_orders
    }
}

#[derive(Parser, Debug)]
#[grammar = "schedule.pest"]
pub struct ScheduleParser;
impl ScheduleParser {
    pub fn parse_content(content: &str) -> Result<ParsedData, ParserError> {
        // get and unwrap the `file` rule; never fails
        let file = ScheduleParser::parse(Rule::file, content)?.next().unwrap();
        let mut task_orders = Vec::new();
        let mut task_durations = Vec::new();

        let mut record_count: usize = 0;
        for record in file.into_inner() {
            match record.as_rule() {
                Rule::record => {
                    record_count += 1;
                    for field in record.into_inner() {
                        ScheduleParser::process_record(
                            field,
                            &mut task_orders,
                            &mut task_durations,
                        );
                    }
                }
                Rule::EOI => (),
                _ => unreachable!(),
            }
        }

        debug!("parsed record_count: {}", record_count);
        debug!("parsed task_durations: {:?}", task_durations);
        debug!("parsed task_orders: {:?}", task_orders);
        Ok(ParsedData {
            task_orders,
            task_durations,
        })
    }

    // `unwraps` here are completely safe as file's adherence to grammar is already
    // verified earlier
    fn process_record<'a>(
        pair: Pair<'a, Rule>,
        task_orders: &mut Vec<(TaskLabel<'a>, Option<TaskLabel<'a>>)>,
        task_durations: &mut Vec<(TaskLabel<'a>, Duration)>,
    ) {
        match pair.as_rule() {
            Rule::task_name_and_duration => {
                let mut pairs = pair.into_inner();
                let (task_name, duration) = parse_task_name_and_duration(&mut pairs);
                task_durations.push((task_name, duration));
                task_orders.push((task_name, None));
            }
            Rule::task_dependencies => {
                let mut pairs = pair.into_inner();
                let task_and_duration_pair = pairs.next().unwrap();
                let (dependent_task_name, duration) =
                    parse_task_name_and_duration(&mut task_and_duration_pair.into_inner());
                task_durations.push((dependent_task_name, duration));
                let task_dependency_list_pair = pairs.next().unwrap();
                for task_name_pair in task_dependency_list_pair.into_inner() {
                    task_orders.push((
                        TaskLabel::new(task_name_pair.as_str()),
                        dependent_task_name.into(),
                    ));
                }
            }
            unknown_term => panic!("Unexpected term: {:?}", unknown_term),
        }
    }
}

fn parse_task_name_and_duration<'a>(pairs: &mut Pairs<'a, Rule>) -> (TaskLabel<'a>, Duration) {
    let name = pairs.next().unwrap();
    let duration = pairs.next().unwrap();
    (
        TaskLabel::new(name.as_str()),
        duration.as_str().parse::<Duration>().unwrap(),
    )
}

#[derive(Debug)]
pub struct ParserError {
    line: usize,
    column: usize,
}

impl StdError for ParserError {}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

impl<R> From<PestError<R>> for ParserError {
    fn from(err: PestError<R>) -> Self {
        let (line_no, col_no) = match err.line_col {
            LineColLocation::Pos(line_col) => line_col,
            LineColLocation::Span(line_col, _) => line_col,
        };
        ParserError {
            line: line_no,
            column: col_no,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskRelation;
    use quickcheck::TestResult;
    use std::collections::HashSet;
    use std::fs;

    const ALLOWED_NON_ALPHABETIC_CHARS: [char; 3] = ['.', '-', '_'];

    #[quickcheck]
    fn task_name_generated_string_succeed(s: String) -> TestResult {
        let gen_str = s
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>();
        if gen_str.is_empty() {
            return TestResult::discard();
        }
        let parsed_task_name = ScheduleParser::parse(Rule::task_name, &gen_str).unwrap();
        TestResult::from_bool(parsed_task_name.as_str() == gen_str)
    }

    #[quickcheck]
    fn task_name_generated_string_fail(c: char) -> TestResult {
        if c.is_alphanumeric() || ALLOWED_NON_ALPHABETIC_CHARS.contains(&c) {
            return TestResult::discard();
        }
        let str = String::from(c);
        let res = ScheduleParser::parse(Rule::task_name, str.as_str());
        TestResult::from_bool(res.is_err())
    }

    #[test]
    fn task_name_succeed() {
        {
            let s = "AA._AA-3312-.zzzLL太_阳";
            let _ = ScheduleParser::parse(Rule::task_name, s).unwrap();

            let s2 = "AA._AA-3312-.zzzLL太_阳////llll";
            let res = ScheduleParser::parse(Rule::task_name, s2).unwrap();
            assert_eq!(res.as_str(), s, "consumption stops at '/'");
        }

        let res = ScheduleParser::parse(Rule::task_name, "A--[-AA太阳").unwrap();
        assert_eq!(res.as_str(), "A--", "only consume until no match");
    }

    #[test]
    fn task_name_fail() {
        let res = ScheduleParser::parse(Rule::task_name, "(AAA");
        assert!(res.is_err(), "Cannot start with a non-letter");

        let res = ScheduleParser::parse(Rule::task_name, "[A");
        assert!(res.is_err(), "Cannot start with a non-letter");
    }

    #[test]
    fn task_name_and_duration_succeed() {
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "A(22)").is_ok());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "A22(22)").is_ok());
        assert!(
            ScheduleParser::parse(Rule::task_name_and_duration, "AA._AA-3312-.zzzLL太_阳(22)")
                .is_ok()
        );
        {
            let mut pairs = ScheduleParser::parse(Rule::task_name_and_duration, "A(022)").unwrap();
            let pair = pairs.next().unwrap();
            let mut pairs = pair.into_inner();
            let (task_name, duration) = parse_task_name_and_duration(&mut pairs);
            assert_eq!(task_name.as_ref(), "A");
            assert_eq!(duration, 22);
        }
    }

    #[test]
    fn task_name_and_duration_fail() {
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "A(2.0)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "A(-22)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "A[(22)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, ")A22(22)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "(A22(22)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "(A22[22)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "[A22[22)").is_err());
        assert!(ScheduleParser::parse(Rule::task_name_and_duration, "A->(2.0)").is_err());
    }

    #[test]
    fn file_parsing() {
        let unparsed_file_content = fs::read_to_string(format!(
            "{}/resources/test/example2.tasks.in",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("Unable to read file to parse");

        let data = ScheduleParser::parse_content(&unparsed_file_content).unwrap();
        assert_eq!(data.task_orders.len(), 15);
        assert_eq!(data.task_durations.len(), 10);

        let all_durations_match = data.task_durations.iter().all(|&(task, dur)| {
            let task_str = task.as_ref();
            if task_str == "方言" {
                dur == 20
            } else if task_str == "锈" {
                dur == 41
            } else {
                dur == 1
            }
        });
        assert!(all_durations_match);

        let expected_orders = vec![
            "Q".node(),
            "Q".arrow("T"),
            "Q".arrow("J"),
            "T".arrow("K"),
            "T".arrow("N"),
            "J".arrow("N"),
            "J".arrow("P"),
            "K".arrow("H"),
            "N".arrow("H"),
            "N".arrow("I"),
            "P".arrow("I"),
            "方言".node(),
            "P".arrow("锈"),
            "J".arrow("锈"),
            "方言".arrow("锈"),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        let orders = data
            .task_orders()
            .iter()
            .cloned()
            .map(|(t1, t2_opt)| t2_opt.map_or(t1.node(), |t2| t1.arrow(t2)))
            .collect::<HashSet<_>>();

        assert_eq!(orders, expected_orders);
    }
}
