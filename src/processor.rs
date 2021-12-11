use crate::analyzer;
use crate::analyzer::ScheduleAnalysis;
use crate::parser::ScheduleParser;
use crate::task::{Duration, TaskLabel, TaskOrder, TaskRelation};
use log::trace;
use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;

pub fn process<'a>(
    unparsed_content: &'a str,
) -> Result<ScheduleAnalysis<'a>, Box<dyn StdError + 'a>> {
    trace!("parsing content...");
    let data = ScheduleParser::parse_content(unparsed_content)?;
    trace!("preparing data for analysis...");
    let task_durations = establish_task_durations(data.task_durations())?;
    let task_orders = establish_task_orders(data.task_orders());
    trace!("analyzing schedule...");
    let analysis = analyzer::analyze_schedule(&task_orders, &task_durations)?;
    Ok(analysis)
}

fn establish_task_durations<'a>(
    task_durations: &[(TaskLabel<'a>, Duration)],
) -> Result<HashMap<TaskLabel<'a>, Duration>, String> {
    let mut same_task_with_different_duration_err = String::new();
    let durations_opt = task_durations.iter().cloned().try_fold(
        HashMap::new(),
        |mut task_durations, (task, duration)| {
            match task_durations.insert(task, duration) {
                // encountered the same task with a different duration ?
                Some(previous_duration) if previous_duration != duration => {
                    same_task_with_different_duration_err.push_str(&format!(
                        "Conflicting durations for task: {}",
                        task.as_ref()
                    ));
                    None
                }
                _ => Some(task_durations),
            }
        },
    );
    match durations_opt {
        Some(durations) => Ok(durations),
        None => Err(same_task_with_different_duration_err),
    }
}

fn establish_task_orders<'a>(
    task_orders: &[(TaskLabel<'a>, Option<TaskLabel<'a>>)],
) -> HashSet<TaskOrder<'a>> {
    task_orders
        .iter()
        .fold(HashSet::new(), |mut orders, &order| {
            match order {
                (first, Some(second)) => orders.insert(first.arrow(second)),
                (first, _) => orders.insert(first.node()),
            };
            orders
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::tests::paths;
    use std::fs;

    lazy_static! {
        static ref TEST_FILE_FOLDER: String =
            format!("{}/resources/test", env!("CARGO_MANIFEST_DIR"));
    }

    #[test]
    fn processing_schedule_from_file_1() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();

        assert_eq!(analysis.max_parallelism(), 3);
        assert_eq!(analysis.task_count(), 8);
        assert_eq!(analysis.minimum_completion_time(), 4);
        assert_eq!(analysis.critical_path_count(), 6);
        assert_eq!(
            analysis.critical_paths(),
            &paths(&[
                "Q->J->N->H",
                "Q->J->N->I",
                "Q->J->P->I",
                "Q->T->K->H",
                "Q->T->N->H",
                "Q->T->N->I"
            ])
        )
    }

    #[test]
    fn processing_schedule_from_file_2() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example2.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 4);
        assert_eq!(analysis.task_count(), 10);
        assert_eq!(analysis.minimum_completion_time(), 61);
        assert_eq!(analysis.critical_path_count(), 1);
        assert_eq!(analysis.critical_paths(), &paths(&["方言->锈"]))
    }

    #[test]
    fn processing_schedule_from_file_3() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example3.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 5);
        assert_eq!(analysis.task_count(), 12);
        assert_eq!(analysis.minimum_completion_time(), 61);
        assert_eq!(analysis.critical_path_count(), 2);
        assert_eq!(analysis.critical_paths(), &paths(&["L->S", "方言->锈"]))
    }

    #[test]
    fn processing_schedule_from_file_4() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example4.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 5);
        assert_eq!(analysis.task_count(), 12);
        assert_eq!(analysis.minimum_completion_time(), 61);
        assert_eq!(analysis.critical_path_count(), 2);
        assert_eq!(analysis.critical_paths(), &paths(&["W->S", "方言->锈"]))
    }

    #[test]
    fn processing_schedule_from_file_5() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example5.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 1);
        assert_eq!(analysis.task_count(), 1);
        assert_eq!(analysis.minimum_completion_time(), 1111);
        assert_eq!(analysis.critical_path_count(), 1);
        assert_eq!(analysis.critical_paths(), &paths(&["A"]))
    }

    #[test]
    fn processing_schedule_from_file_6() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example6.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 1);
        assert_eq!(analysis.task_count(), 2);
        assert_eq!(analysis.minimum_completion_time(), 103);
        assert_eq!(analysis.critical_path_count(), 1);
        assert_eq!(analysis.critical_paths(), &paths(&["B->A"]))
    }

    #[test]
    fn processing_schedule_from_file_7() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example7.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 3);
        assert_eq!(analysis.task_count(), 3);
        assert_eq!(analysis.minimum_completion_time(), 0);
        assert_eq!(analysis.critical_path_count(), 3);
        assert_eq!(analysis.critical_paths(), &paths(&["A", "B", "C"]))
    }

    #[test]
    fn processing_schedule_from_file_8() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example8.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 2);
        assert_eq!(analysis.task_count(), 2);
        assert_eq!(analysis.minimum_completion_time(), 17);
        assert_eq!(analysis.critical_path_count(), 1);
        assert_eq!(analysis.critical_paths(), &paths(&["B"]))
    }

    #[test]
    fn processing_schedule_from_file_9() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example9.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();
        assert_eq!(analysis.max_parallelism(), 1);
        assert_eq!(analysis.task_count(), 2);
        assert_eq!(analysis.minimum_completion_time(), 37);
        assert_eq!(analysis.critical_path_count(), 1);
        assert_eq!(analysis.critical_paths(), &paths(&["B->A"]))
    }

    #[test]
    #[should_panic(expected = "Cycle")]
    fn processing_schedule_from_file_10() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example10.tasks.in")).unwrap();
        let _ = process(&unparsed_content).unwrap();
    }

    #[test]
    #[should_panic(expected = "MissingDurations([TL(B), TL(C)]")]
    fn processing_schedule_from_file_11() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example11.tasks.in")).unwrap();
        let _ = process(&unparsed_content).unwrap();
    }

    #[test]
    #[should_panic(expected = "EmptyInput")]
    fn processing_schedule_from_file_12() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example12.tasks.in")).unwrap();
        let _ = process(&unparsed_content).unwrap();
    }

    #[test]
    #[should_panic(expected = "Conflicting durations")]
    fn processing_schedule_from_file_13() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example13.tasks.in")).unwrap();
        let _ = process(&unparsed_content).unwrap();
    }

    #[test]
    #[should_panic(expected = "line: 2, column: 6")]
    fn processing_schedule_from_file_14() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example14.tasks.in")).unwrap();
        let _ = process(&unparsed_content).unwrap();
    }

    #[test]
    #[should_panic(expected = "Labels cannot have a dependency on themselves")]
    fn processing_schedule_from_file_15() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example15.tasks.in")).unwrap();
        let _ = process(&unparsed_content).unwrap();
    }

    #[test]
    fn processing_schedule_from_file_16() {
        let unparsed_content =
            fs::read_to_string(format!("{}/{}", *TEST_FILE_FOLDER, "example16.tasks.in")).unwrap();
        let analysis = process(&unparsed_content).unwrap();

        assert_eq!(
            analysis.critical_paths(),
            &paths(&["v8KK2w5u6a72cQmFVJph88->hV4qcwM0JWUb97yFkKfYcK75DL->t3e49256a01B8W1DG8m37c->BuoxppGpYOk9kdzEAELC7o9B"])
        );
    }
}
