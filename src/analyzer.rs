use crate::task::{Duration, TaskLabel, TaskOrder, TotalDuration};
use log::{debug, trace};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;
use std::fmt::Formatter;
use std::fmt::Write;

/// Uses Kahn's topological sorting algorithm to analyze acyclic schedules. It recognizes the fact
/// that a finite DAG has at least one source and at least one sink. It is capable of detecting
/// cycles, which results in AnalysisError::Cycle

#[derive(Debug)]
pub struct ScheduleAnalysis<'a> {
    max_parallelism: usize,
    task_count: usize,
    minimum_completion_time: TotalDuration,
    critical_path_count: usize,
    critical_paths: Vec<Vec<TaskLabel<'a>>>,
}

#[allow(dead_code)]
impl<'a> ScheduleAnalysis<'a> {
    pub fn max_parallelism(&self) -> usize {
        self.max_parallelism
    }

    pub fn task_count(&self) -> usize {
        self.task_count
    }

    pub fn minimum_completion_time(&self) -> TotalDuration {
        self.minimum_completion_time
    }

    pub fn critical_path_count(&self) -> usize {
        self.critical_path_count
    }

    pub fn critical_paths(&self) -> &Vec<Vec<TaskLabel<'a>>> {
        &self.critical_paths
    }
}

impl<'a> std::fmt::Display for ScheduleAnalysis<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "task_count: {}", self.task_count)?;
        writeln!(f, "max_parallelism: {}", self.max_parallelism)?;
        writeln!(
            f,
            "minimum_completion_time: {}",
            self.minimum_completion_time
        )?;
        writeln!(f, "critical_path_count: {}", self.critical_path_count)?;
        writeln!(
            f,
            "critical_path{}:",
            if self.critical_path_count > 1 {
                "s"
            } else {
                ""
            }
        )?;
        for (path_idx, path) in self.critical_paths.iter().enumerate() {
            if self.critical_path_count > 1 {
                writeln!(f, "{})", path_idx + 1)?;
            }
            serialize_path(path, f, "->", TaskLabel::MAX_LEN)?;
            let not_last_path = path_idx != self.critical_path_count - 1;
            if not_last_path {
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum AnalysisError<'a> {
    EmptyInput,
    MissingDurations(Vec<TaskLabel<'a>>),
    MissingOrders(Vec<TaskLabel<'a>>),
    Cycle,
}

impl<'a> StdError for AnalysisError<'a> {}

impl<'a> fmt::Display for AnalysisError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_analysis_error(self, f)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct TaskExecutionEndTime<'a> {
    task: TaskLabel<'a>,
    end_time: TotalDuration,
}

impl<'a> PartialOrd for TaskExecutionEndTime<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.end_time.partial_cmp(&other.end_time)
    }
}

impl<'a> Ord for TaskExecutionEndTime<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.end_time.cmp(&other.end_time)
    }
}

fn format_analysis_error<'a>(err: &AnalysisError<'a>, f: &mut fmt::Formatter) -> fmt::Result {
    match err {
        AnalysisError::EmptyInput => write!(f, "Input is empty"),
        AnalysisError::MissingDurations(vec) => {
            write!(
                f,
                "Schedule is missing durations for: {:?}",
                vec.iter().map(|tl| tl.as_ref()).collect::<Vec<_>>()
            )
        }
        AnalysisError::MissingOrders(vec) => {
            write!(
                f,
                "Schedule is missing orders for: {:?}",
                vec.iter().map(|tl| tl.as_ref()).collect::<Vec<_>>()
            )
        }
        AnalysisError::Cycle => write!(f, "There's a cycle in the schedule"),
    }
}

/// Produces an analysis of provided task schedule
/// Time: O((V+E)logV) for topological sorting that uses a binary heap to figure out
///       maximum parallel task execution, where V is the number of
///       tasks and E is the number of relations between tasks. However,
///       using a fibonacci heap could bring the cost down to O(E + VlogV).
///       See https://docs.rs/rudac/0.8.3/rudac/heap/struct.FibonacciHeap.html
///       We need to add to this the additional cost of multiple critical path construction,
///       which is ~O(N^M * M). See "construct_path" for more explanation on that.
/// Space: O(V). Might end up adding all tasks to the queue at once
pub fn analyze_schedule<'a>(
    task_orders: &HashSet<TaskOrder<'a>>,
    task_durations: &HashMap<TaskLabel<'a>, Duration>,
) -> Result<ScheduleAnalysis<'a>, AnalysisError<'a>> {
    if task_orders.is_empty() && task_durations.is_empty() {
        return Err(AnalysisError::EmptyInput);
    }
    let Graph {
        task_graph,
        mut preceding_task_count,
    } = Graph::new(task_orders);
    {
        let mut missing = preceding_task_count
            .keys()
            .filter(|&task| !task_durations.contains_key(task))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(AnalysisError::MissingDurations(missing));
        }
    }

    if task_durations.len() != preceding_task_count.len() {
        let mut missing = task_durations
            .keys()
            .filter(|&task| !preceding_task_count.contains_key(task))
            .cloned()
            .collect::<Vec<_>>();
        missing.sort_unstable();
        return Err(AnalysisError::MissingOrders(missing));
    }

    debug!("created task_graph: {:?}", task_graph);
    debug!("created preceding_task_count: {:?}", preceding_task_count);
    // using heap to figure out the maximum number of tasks that can be run simultaneously
    let mut task_queue = BinaryHeap::new();
    // longest time spent, including the task's own duration, along the path to reach the task
    // via one of the paths
    let mut longest_duration_path_to_task = HashMap::new();
    for (&task, count) in &preceding_task_count {
        let source_task = *count == 0;
        if source_task {
            task_queue.push(Reverse(TaskExecutionEndTime {
                task,
                end_time: task_durations[&task] as TotalDuration,
            }));
            longest_duration_path_to_task.insert(task, task_durations[&task] as TotalDuration);
        }
    }
    {
        let no_source_tasks_exist = task_queue.is_empty();
        if no_source_tasks_exist {
            return Err(AnalysisError::Cycle);
        }
    }
    debug!("source_tasks: {:?}", task_queue);
    let mut max_parallel_tasks = 0usize;
    let mut sink_tasks = Vec::new(); // they do not precede any tasks
    let mut parent_tasks = HashMap::new();
    while !task_queue.is_empty() {
        max_parallel_tasks = max_parallel_tasks.max(task_queue.len());
        let TaskExecutionEndTime {
            task: from_task, ..
        } = task_queue.pop().unwrap().0;
        // Given two paths such as ["A", "C -> K -> L"], "A" is a single-path task. "C" and "K"
        // precede other tasks; C needs to be executed before K, and K needs to be executed before "L"
        // L is a "sink" task. A is also a "sink" task due to being the last task to execute on the path.
        let single_task_path_or_precedes_other_tasks = task_graph.contains_key(&from_task);
        if single_task_path_or_precedes_other_tasks {
            let adjacent_tasks = &task_graph[&from_task];
            let path_with_single_task = adjacent_tasks.is_empty();
            if path_with_single_task {
                sink_tasks.push(from_task);
            }
            for &to_task in adjacent_tasks {
                let alternative_path_duration = longest_duration_path_to_task[&from_task]
                    + task_durations[&to_task] as TotalDuration;
                if let Some(&previous_path_duration) = longest_duration_path_to_task.get(&to_task) {
                    // relaxing path duration
                    if alternative_path_duration > previous_path_duration {
                        longest_duration_path_to_task.insert(to_task, alternative_path_duration);
                        parent_tasks.insert(to_task, vec![from_task]);
                    } else if alternative_path_duration == previous_path_duration {
                        parent_tasks
                            .entry(to_task)
                            .and_modify(|vec| vec.push(from_task));
                    }
                } else {
                    longest_duration_path_to_task.insert(to_task, alternative_path_duration);
                    parent_tasks.insert(to_task, vec![from_task]);
                }
                preceding_task_count
                    .entry(to_task)
                    .and_modify(|count| *count -= 1);
                let ready_to_schedule = preceding_task_count[&to_task] == 0;
                if ready_to_schedule {
                    task_queue.push(Reverse(TaskExecutionEndTime {
                        task: to_task,
                        end_time: longest_duration_path_to_task[&to_task],
                    }));
                }
            }
        } else {
            sink_tasks.push(from_task);
        }
    }

    // being extra careful
    let no_cycle_exists = preceding_task_count.values().all(|&count| count == 0);
    if no_cycle_exists {
        trace!("finding critical paths...");
        let CriticalPaths {
            paths: critical_paths,
            duration: critical_path_duration,
        } = CriticalPaths::find_critical_paths(
            &parent_tasks,
            &longest_duration_path_to_task,
            &sink_tasks,
        );
        debug!("critical paths:{:?}", critical_paths);
        Ok(ScheduleAnalysis {
            max_parallelism: max_parallel_tasks,
            task_count: preceding_task_count.len(),
            critical_path_count: critical_paths.len(),
            minimum_completion_time: critical_path_duration,
            critical_paths,
        })
    } else {
        Err(AnalysisError::Cycle)
    }
}

#[derive(Debug)]
struct Graph<'a> {
    task_graph: HashMap<TaskLabel<'a>, Vec<TaskLabel<'a>>>, // task -> neighbors
    preceding_task_count: HashMap<TaskLabel<'a>, usize>,    // task -> number of preceding tasks
}

impl<'a> Graph<'a> {
    fn new(orders: &HashSet<TaskOrder<'a>>) -> Self {
        let mut preceding_task_count = HashMap::new(); // aka, preceding_edge_count
        let mut task_graph = HashMap::new();
        for task_order in orders {
            // make sure all nodes/tasks have an "incoming edge"/"preceding task" count,
            // including the sources at the head of the graph
            preceding_task_count
                .entry(task_order.first())
                .or_insert(0usize);
            let adj_list = task_graph
                .entry(task_order.first())
                .or_insert_with(Vec::new);
            task_order.second().iter().for_each(|&second| {
                adj_list.push(second);
                *preceding_task_count.entry(second).or_insert(0usize) += 1;
            });
        }
        Graph {
            task_graph,
            preceding_task_count,
        }
    }
}

#[derive(Debug)]
struct CriticalPaths<'a> {
    paths: Vec<Vec<TaskLabel<'a>>>,
    duration: TotalDuration,
}

impl<'a> CriticalPaths<'a> {
    // If there are multiple CPs, the ones that have more tasks on them come before in order.
    // Else, we defer to lexicographical order of paths' task labels.

    fn find_critical_paths(
        parent_tasks: &HashMap<TaskLabel<'a>, Vec<TaskLabel<'a>>>,
        longest_duration_path_to_task: &HashMap<TaskLabel<'a>, TotalDuration>,
        sink_tasks: &[TaskLabel<'a>],
    ) -> Self {
        debug!("parent_tasks: {:?}", parent_tasks);
        debug!(
            "longest_duration_path_to_task: {:?}",
            longest_duration_path_to_task
        );
        debug!("sink_tasks: {:?}", sink_tasks);
        let critical_path_duration = sink_tasks
            .iter()
            .map(|task| longest_duration_path_to_task[task])
            .max()
            .unwrap_or(0);

        // Derive CPs from each sink task
        let mut critical_paths = sink_tasks
            .iter()
            .filter(|&task| longest_duration_path_to_task[task] == critical_path_duration)
            .map(|&task| {
                let mut paths = Vec::new();
                CriticalPaths::construct_paths(parent_tasks, &mut paths, &mut Vec::new(), task);
                paths.iter_mut().for_each(|path| path.reverse());
                paths
            })
            .flatten()
            .collect::<Vec<_>>();

        // Paths with more tasks should come first because they provide more opportunities
        // for optimization. Else, we defer to lexicographical ordering.
        critical_paths.sort_unstable_by(|path1, path2| {
            path2.len().cmp(&path1.len())
                .then(path1.iter().cmp(path2.iter()))
                .then_with(|| panic!("There cannot be duplicate critical paths {:?}", path1))
        });
        CriticalPaths {
            paths: critical_paths,
            duration: critical_path_duration,
        }
    }

    // Time: O(n^m * m), where n is max_len(parent_tasks.values()) and m is the total number of
    //       tasks on the CP. "*m" comes from path additions while cloning
    // Space: O(m) for stack space
    fn construct_paths(
        parent_tasks: &HashMap<TaskLabel<'a>, Vec<TaskLabel<'a>>>,
        paths: &mut Vec<Vec<TaskLabel<'a>>>,
        temp_path: &mut Vec<TaskLabel<'a>>,
        destination: TaskLabel<'a>,
    ) {
        let reached_source = !parent_tasks.contains_key(&destination);
        if reached_source {
            {
                let path_with_single_task = temp_path.is_empty();
                if path_with_single_task {
                    temp_path.push(destination);
                }
            }
            paths.push(temp_path.clone());
        } else {
            {
                let is_sink_task = temp_path.is_empty();
                if is_sink_task {
                    temp_path.push(destination);
                }
            }
            for &task in &parent_tasks[&destination] {
                temp_path.push(task);
                CriticalPaths::construct_paths(parent_tasks, paths, temp_path, task);
                temp_path.pop(); // unwinding the stack
            }
        }
    }
}

fn serialize_path(
    path: &[TaskLabel],
    buffer: &mut dyn Write,
    delimiter: &str,
    max_label_len: usize,
) -> std::fmt::Result {
    let delimiter_len = delimiter.chars().count();
    let mut buffered_char_count = 0usize;
    let max_allowed_line_len = max_label_len + delimiter_len;

    let mut line_buffer = String::new();
    let mut label_idx = 0usize;
    while label_idx < path.len() {
        let task = path[label_idx];
        let task_len = task.chars().count();
        let required_space = task_len + delimiter_len;
        if buffered_char_count + required_space <= max_allowed_line_len {
            line_buffer.push_str(task.as_ref());
            let not_last_label = label_idx != path.len() - 1;
            if not_last_label {
                line_buffer.push_str(delimiter);
            }
            buffered_char_count += required_space;
            label_idx += 1;
        } else {
            writeln!(buffer, "{}", line_buffer)?;
            line_buffer.clear();
            buffered_char_count = 0;
        }
    }
    // flush out the remaining
    writeln!(buffer, "{}", line_buffer)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::task::{TaskLabel, TaskRelation};
    use quickcheck;
    use quickcheck::TestResult;
    use std::convert::TryFrom;
    use util::*;

    #[test]
    fn single_task_path_schedules() {
        // single-task path
        let ords = &["A".node()];
        let durs = &[("A", 2)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 1);
        assert_eq!(analysis.task_count, 1);
        assert_eq!(analysis.minimum_completion_time, 2);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["A"]));

        // two single-task paths
        let ords = &["A".node(), "B".node()];
        let durs = &[("A", 2), ("B", 3)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 2);
        assert_eq!(analysis.task_count, 2);
        assert_eq!(analysis.minimum_completion_time, 3);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["B"]));

        // three paths, two of which are a single-task path
        // A
        // B
        // D -> L
        let ords = &["A".node(), "B".node(), "D".arrow("L")];
        let durs = &[("A", 2), ("B", 3), ("D", 7), ("L", 1)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 3);
        assert_eq!(analysis.task_count, 4);
        assert_eq!(analysis.minimum_completion_time, 8);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["D->L"]));
    }

    #[test]
    fn multiple_sources_and_multiple_sinks_path_schedules() {
        // A -> C
        // B -> D
        let ords = &["A".arrow("C"), "B".arrow("D")];
        let durs = &[("A", 5 as Duration), ("B", 1), ("C", 9), ("D", 7)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 2);
        assert_eq!(analysis.task_count, 4);
        assert_eq!(analysis.minimum_completion_time, 14);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["A->C"]));

        // A -> C
        // B -> D
        let ords = &["A".arrow("C"), "B".arrow("D")];
        let durs = &[("A", 5 as Duration), ("B", 7), ("C", 9), ("D", 8)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 2);
        assert_eq!(analysis.task_count, 4);
        assert_eq!(analysis.minimum_completion_time, 15);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["B->D"]));
    }

    #[test]
    fn report_accurate_parallelism_as_time_progresses() {
        //                /--> D
        //               /
        //  A --> B --> C --> E
        //              \
        //               \--> F
        //  K
        let ords = &[
            "A".arrow("B"),
            "B".arrow("C"),
            "C".arrow("D"),
            "C".arrow("E"),
            "C".arrow("F"),
            "K".node(),
        ];
        let durs = &[
            ("A", 1 as Duration),
            ("B", 1),
            ("C", 1),
            ("D", 1),
            ("E", 1),
            ("F", 1),
            ("K", 4),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(
            analysis.max_parallelism, 4,
            "finding tasks D, E, F, K running together at the 4th \"tick\" requires 4 task-runners"
        );
        assert_eq!(analysis.task_count, 7);
        assert_eq!(analysis.minimum_completion_time, 4);
        assert_eq!(analysis.critical_path_count, 4);
        assert_eq!(
            analysis.critical_paths,
            paths(&["A->B->C->D", "A->B->C->E", "A->B->C->F", "K"])
        );

        let ords = &[
            "A".arrow("B"),
            "B".arrow("C"),
            "C".arrow("D"),
            "C".arrow("E"),
            "C".arrow("F"),
            "K".node(),
        ];
        let durs = &[
            ("A", 1 as Duration),
            ("B", 1),
            ("C", 1),
            ("D", 1),
            ("E", 1),
            ("F", 1),
            ("K", 3),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(
            analysis.max_parallelism, 3,
            "K finishes before we get to execute D, E, F at the 4th tick, thus at most 3 task-runners needed"
        );
        assert_eq!(analysis.task_count, 7);
        assert_eq!(analysis.minimum_completion_time, 4);
        assert_eq!(analysis.critical_path_count, 3);
        assert_eq!(
            analysis.critical_paths,
            paths(&["A->B->C->D", "A->B->C->E", "A->B->C->F"])
        );

        let ords = &["A".arrow("B"), "A".arrow("C"), "K".node()];
        let durs = &[("A", 0 as Duration), ("B", 0), ("C", 0), ("K", 0)];
        let analysis = analyze(ords, durs).unwrap();
        assert!(
            analysis.max_parallelism == 2 || analysis.max_parallelism == 3,
            "Time does not exist; edge case!!!"
        );
        assert_eq!(analysis.task_count, 4);
        assert_eq!(analysis.minimum_completion_time, 0);
        assert_eq!(analysis.critical_path_count, 3);
        assert_eq!(analysis.critical_paths, paths(&["A->B", "A->C", "K"]));
    }

    #[test]
    fn single_source_and_multiple_sinks_path_schedules() {
        //    /--> L -> Z
        //   /
        //  K
        //   \
        //    \--> T -> F
        let ords = &[
            "K".arrow("L"),
            "K".arrow("T"),
            "L".arrow("Z"),
            "T".arrow("F"),
        ];
        let durs = &[
            ("K", 1 as Duration),
            ("L", 12),
            ("Z", 1),
            ("T", 5),
            ("F", 20),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 2);
        assert_eq!(analysis.task_count, 5);
        assert_eq!(analysis.minimum_completion_time, 26);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["K->T->F"]));

        // All CPs have equal duration, lexicographically smaller ones come
        // first in order in the result set.
        //    /--> B -> D ->- >H
        //   /     \        /
        //  A       > --- >F         -> I
        //   \     /                /
        //    \--> C -> G -------->
        let ords = &[
            "A".arrow("B"),
            "A".arrow("C"),
            "B".arrow("D"),
            "B".arrow("F"),
            "C".arrow("F"),
            "C".arrow("G"),
            "F".arrow("H"),
            "D".arrow("H"),
            "G".arrow("I"),
        ];
        let durs = &[
            ("A", 1 as Duration),
            ("B", 1),
            ("C", 1),
            ("D", 1),
            ("F", 1),
            ("H", 1),
            ("G", 1),
            ("I", 1),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 3);
        assert_eq!(analysis.task_count, 8);
        assert_eq!(analysis.minimum_completion_time, 4);
        assert_eq!(analysis.critical_path_count, 4);
        assert_eq!(
            analysis.critical_paths,
            paths(&["A->B->D->H", "A->B->F->H", "A->C->F->H", "A->C->G->I"])
        );

        // All CPs have equal duration, lexicographically smaller ones come first.
        //    /--> B -> D ->- >H
        //   /     \        /
        //  A       > --- >F --->---> I
        //   \     /                /
        //    \--> C -> G -------->
        let ords = &[
            "A".arrow("B"),
            "A".arrow("C"),
            "B".arrow("D"),
            "B".arrow("F"),
            "C".arrow("F"),
            "C".arrow("G"),
            "F".arrow("H"),
            "D".arrow("H"),
            "G".arrow("I"),
            "F".arrow("I"),
        ];
        let durs = &[
            ("A", 1 as Duration),
            ("B", 1),
            ("C", 1),
            ("D", 1),
            ("F", 1),
            ("H", 1),
            ("G", 1),
            ("I", 1),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 3);
        assert_eq!(analysis.task_count, 8);
        assert_eq!(analysis.minimum_completion_time, 4);
        assert_eq!(analysis.critical_path_count, 6);
        assert_eq!(
            analysis.critical_paths,
            paths(&[
                "A->B->D->H",
                "A->B->F->H",
                "A->B->F->I",
                "A->C->F->H",
                "A->C->F->I",
                "A->C->G->I"
            ])
        );

        // All CPs have equal duration.
        //    /--> B -> D ->- >H
        //   /     \        /
        //  A       > --- >F --->---> I --> K
        //   \     /                /
        //    \--> C -> G -------->
        let ords = &[
            "A".arrow("B"),
            "A".arrow("C"),
            "B".arrow("D"),
            "B".arrow("F"),
            "C".arrow("F"),
            "C".arrow("G"),
            "F".arrow("H"),
            "D".arrow("H"),
            "G".arrow("I"),
            "F".arrow("I"),
            "I".arrow("K"),
        ];
        let durs = &[
            ("A", 1 as Duration),
            ("B", 1),
            ("C", 1),
            ("D", 1),
            ("F", 1),
            ("H", 1),
            ("G", 1),
            ("I", 1),
            ("K", 0),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 3);
        assert_eq!(analysis.task_count, 9);
        assert_eq!(analysis.minimum_completion_time, 4);
        assert_eq!(analysis.critical_path_count, 6);
        assert_eq!(
            analysis.critical_paths,
            paths(&[
                "A->B->F->I->K",
                "A->C->F->I->K",
                "A->C->G->I->K",
                "A->B->D->H",
                "A->B->F->H",
                "A->C->F->H"
            ])
        );
    }

    #[test]
    fn multiple_sources_and_single_sink_path_schedules() {
        // P -> T ->
        //           \
        // Z ------>  > D
        //            /
        //           /
        // J ----->
        let ords = &[
            "P".arrow("T"),
            "T".arrow("D"),
            "Z".arrow("D"),
            "J".arrow("D"),
        ];
        let durs = &[
            ("P", 7 as Duration),
            ("T", 19),
            ("D", 0),
            ("Z", 10),
            ("J", 26),
        ];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 3);
        assert_eq!(analysis.task_count, 5);
        assert_eq!(analysis.minimum_completion_time, 26);
        assert_eq!(analysis.critical_path_count, 2);
        assert_eq!(analysis.critical_paths, paths(&["P->T->D", "J->D"]));
    }

    #[test]
    fn zero_durations_and_no_task_ordering() {
        let ords = &["A".node(), "B".node(), "C".node(), "D".node()];
        let durs = &[("A", 0), ("B", 0), ("C", 0), ("D", 0)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 4);
        assert_eq!(analysis.task_count, 4);
        assert_eq!(analysis.minimum_completion_time, 0);
        assert_eq!(analysis.critical_path_count, 4);
        assert_eq!(analysis.critical_paths, paths(&["A", "B", "C", "D"]));
    }

    #[test]
    fn flexible_fusion() {
        // A -> B, where A is being fused to B later
        let ords = &["A".node(), "B".node(), "A".arrow("B")];
        let durs = &[("A", 2), ("B", 1)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 1);
        assert_eq!(analysis.task_count, 2);
        assert_eq!(analysis.minimum_completion_time, 3);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["A->B"]));

        // A -> B
        let ords = &["A".arrow("B")];
        let durs = &[("A", 2), ("B", 1)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 1);
        assert_eq!(analysis.task_count, 2);
        assert_eq!(analysis.minimum_completion_time, 3);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["A->B"]));

        // A -> B -> D, where A and B is fused later
        let ords = &["A".node(), "B".node(), "B".arrow("D"), "A".arrow("B")];
        let durs = &[("A", 2), ("B", 1), ("D", 3)];
        let analysis = analyze(ords, durs).unwrap();
        assert_eq!(analysis.max_parallelism, 1);
        assert_eq!(analysis.task_count, 3);
        assert_eq!(analysis.minimum_completion_time, 6);
        assert_eq!(analysis.critical_path_count, 1);
        assert_eq!(analysis.critical_paths, paths(&["A->B->D"]));
    }

    #[test]
    fn empty_input() {
        let ords = &[];
        let durs = &[];
        let res = analyze(ords, durs);
        assert!(matches!(res, Err(AnalysisError::EmptyInput)));
    }

    #[test]
    fn missing_durations() {
        let ords = &["A".node(), "B".node(), "D".arrow("L")];
        let durs = &[("A", 2), ("L", 1)];
        let res = analyze(ords, durs);
        match res {
            Err(AnalysisError::MissingDurations(vec)) => assert_eq!(vec, labels(&["B", "D"])),
            other => assert!(matches!(other, Err(AnalysisError::MissingDurations(_)))),
        }

        let ords = &["A".node(), "B".node(), "D".arrow("L")];
        let durs = &[];
        let res = analyze(ords, durs);
        match res {
            Err(AnalysisError::MissingDurations(vec)) => {
                assert_eq!(vec, labels(&["A", "B", "D", "L"]))
            }
            other => {
                assert!(matches!(other, Err(AnalysisError::MissingDurations(_))));
            }
        }
    }

    #[test]
    fn missing_orders() {
        let ords = &["A".node(), "D".arrow("L")];
        let durs = &[("A", 2), ("B", 3), ("D", 7), ("L", 1)];
        let res = analyze(ords, durs);
        match res {
            Err(AnalysisError::MissingOrders(vec)) => assert_eq!(vec, labels(&["B"])),
            other => assert!(matches!(other, Err(AnalysisError::MissingOrders(_)))),
        }

        let ords = &[];
        let durs = &[("A", 2), ("L", 1)];
        let res = analyze(ords, durs);
        match res {
            Err(AnalysisError::MissingOrders(vec)) => assert_eq!(vec, labels(&["A", "L"])),
            other => assert!(matches!(other, Err(AnalysisError::MissingOrders(_)))),
        }
    }

    #[quickcheck]
    fn simple_auto_generated_schedules(
        gen_labels: HashSet<String>,
        gen_durations: Vec<Duration>,
    ) -> TestResult {
        {
            let gen_labels_len = gen_labels.len();
            if gen_labels_len < 20 || gen_labels_len > 100 {
                return TestResult::discard();
            }

            if gen_durations.len() < gen_labels_len {
                return TestResult::discard();
            }
        }

        let str_labels = gen_labels
            .iter()
            .filter(|s| TaskLabel::try_from(s.as_str()).is_ok())
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        if str_labels.is_empty() {
            return TestResult::discard();
        }

        let task_count = str_labels.len();
        let durations = gen_durations
            .iter()
            .cloned()
            .take(task_count)
            .collect::<Vec<_>>();
        let max_duration = *durations.iter().max().unwrap();
        let mut critical_paths = str_labels
            .iter()
            .cloned()
            .zip(gen_durations.iter().cloned())
            .fold(
                Vec::new(),
                |mut paths: Vec<Vec<TaskLabel>>, (label, dur)| {
                    if dur == max_duration {
                        paths.push(vec![TaskLabel::new(label)]);
                    }
                    paths
                },
            );

        critical_paths.sort_unstable_by(|path1, path2| {
            path1
                .iter()
                .zip(path2.iter())
                .map(|(str1, str2)| str1.cmp(str2))
                .skip_while(|cmp| *cmp == Ordering::Equal)
                .next()
                .unwrap()
        });

        let ords = str_labels.iter().map(|l| l.node()).collect::<Vec<_>>();
        let durs = str_labels
            .into_iter()
            .zip(gen_durations.into_iter())
            .collect::<Vec<_>>();
        let analysis = analyze(&ords, &durs).unwrap();
        assert_eq!(analysis.max_parallelism, task_count);
        assert_eq!(analysis.task_count, task_count);
        assert_eq!(
            analysis.minimum_completion_time,
            max_duration as TotalDuration
        );
        assert_eq!(analysis.critical_path_count, critical_paths.len());
        assert_eq!(analysis.critical_paths, critical_paths);
        TestResult::passed()
    }

    #[test]
    fn cyclic_schedules() {
        // A -> B -> A
        let ords = &["A".arrow("B"), "B".arrow("A")];
        let durs = &[("A", 5 as Duration), ("B", 1)];
        let res = analyze(ords, durs);
        assert_eq!(res.unwrap_err(), AnalysisError::Cycle);

        // A -> C
        //        \
        // B ----- -> D -> A
        let ords = &[
            "A".arrow("C"),
            "B".arrow("D"),
            "C".arrow("D"),
            "D".arrow("A"),
        ];
        let durs = &[("A", 5 as Duration), ("B", 1), ("C", 1), ("D", 7)];
        let res = analyze(ords, durs);
        assert_eq!(res.unwrap_err(), AnalysisError::Cycle);

        // A -> C -> D -> B -> A
        let ords = &[
            "A".arrow("C"),
            "C".arrow("D"),
            "D".arrow("B"),
            "B".arrow("A"),
        ];
        let durs = &[("A", 5 as Duration), ("B", 1), ("C", 1), ("D", 7)];
        let res = analyze(ords, durs);
        assert_eq!(res.unwrap_err(), AnalysisError::Cycle);

        //       --> L --->
        //      /         |
        // K -> ---> T --->
        let ords = &[
            "K".arrow("L"),
            "K".arrow("T"),
            "L".arrow("T"),
            "T".arrow("L"),
        ];
        let durs = &[("K", 5 as Duration), ("L", 1), ("T", 1)];
        let res = analyze(ords, durs);
        assert_eq!(res.unwrap_err(), AnalysisError::Cycle);
    }

    #[test]
    fn path_serialization() {
        let path = labels(&["B", "D", "C"]);
        let mut buf = String::new();
        let _ = serialize_path(&path, &mut buf, "->", 1);
        let vec_str = buf.split_whitespace().collect::<Vec<&str>>();
        assert_eq!(vec_str[0], "B->");
        assert_eq!(vec_str[1], "D->");
        assert_eq!(vec_str[2], "C");

        let path = labels(&["BB", "DD", "CC"]);
        let mut buf = String::new();
        let _ = serialize_path(&path, &mut buf, "->", 2);
        let vec_str = buf.split_whitespace().collect::<Vec<&str>>();
        assert_eq!(vec_str[0], "BB->");
        assert_eq!(vec_str[1], "DD->");
        assert_eq!(vec_str[2], "CC");

        let path = labels(&["BB"]);
        let mut buf = String::new();
        let _ = serialize_path(&path, &mut buf, "->", 2);
        let vec_str = buf.split_whitespace().collect::<Vec<&str>>();
        assert_eq!(vec_str[0], "BB");
    }

    #[quickcheck]
    fn path_serialization_with_generated_input(vec: Vec<String>) -> TestResult {
        let path_strs = vec
            .iter()
            .map(|s| {
                s.chars()
                    .filter(|c| c.is_alphanumeric())
                    .collect::<String>()
            })
            .collect::<Vec<String>>();

        let path = path_strs
            .iter()
            .filter_map(|s| TaskLabel::try_from(s.as_str()).ok())
            .collect::<Vec<_>>();

        let mut buf = String::new();
        let delimiter = "->";
        let _ = serialize_path(&path, &mut buf, delimiter, TaskLabel::MAX_LEN);
        let delimeter_len = delimiter.chars().count();
        buf.split_whitespace()
            .all(|s| s.len() <= TaskLabel::MAX_LEN + delimeter_len);
        TestResult::passed()
    }

    #[test]
    fn path_serialization_custom_labels() {
        let path = [
            "0e928v8U8vJ8136qq",
            "VO2JI",
            "oNdK9v0L8HVsf",
            "GSIDD3BBY5s92KwO92L7Z",
            "BH9Iwo0",
            "g0y4s",
            "5W0m5D1586o8KM",
            "p9T80Q3IMl4v3RVo9z1L",
            "7o9Lffql1ByrSN6Nw9B9g8h3t",
            "bzfX40xVStq3BmNYhz19LN",
            "rYPfT7W9BT195uW2JLr",
            "P5GMQsLs0pmQ71",
            "4IX55y2Z03",
            "x4nXd",
            "1vRC03Gp4",
            "XDpK",
            "5Y5QX9Sr",
            "HDS46bzvn4",
            "I2a",
            "P",
            "we52ma8",
            "3L606Qbq0x4xlj4504xYD5",
            "YkIe19i7bDe4",
            "0",
            "Lq7NHYotR365uANzrp0",
            "e9919B6knL38E2",
            "uc8G",
            "Sf1pUx1FpaC0gDQR11t",
            "G3UIv7Nxq29Z",
            "7xw",
            "c",
            "w4eAY4Xc27tl0PJ",
            "du5",
            "1e3",
            "imm4",
            "4Rqc",
            "ha6K6",
            "h7ygXHvs0",
            "kl9R5Zhg8PLbg5CQ8S22n",
            "6FxlsD8c3",
            "BuoxppGpYOk9kdzEAELC7o9B",
            "7pjk1WX9XDKafb9ZuMCq",
            "eVpqqtLkx552s27A",
            "5O",
            "7f4o0mYisAvtN8QW4b71",
            "Y3D4P",
            "TzniQk0vbH6W23JNW2iv",
            "F956Fm5iVk4I32r",
            "jWS8W8euiV5sW8fd8S",
            "X7jdHFfjk79B6G0z7094Ez97G8OX",
            "532garQ3GytE",
            "OvA48Av",
            "78B1A7y",
            "az",
            "p72kp",
            "3QCK",
            "1sK8",
            "z",
            "TvJF92ZQUh",
            "v8KK2w5u6a72cQmFVJph88",
            "1CFEtP8k4pf8G0t",
            "IBk2Y6g3H",
            "aG4",
            "47f08419eV",
            "hV4qcwM0JWUb97yFkKfYcK75DL",
            "RfbD1Cv6Y7ThmTVasf",
            "Xrp12YvQnZ6",
            "G2xe78a5mkXh0FeA",
            "13cER4Bq7X290024",
            "B",
            "DrKrfJ",
            "wz29wPI5S4",
            "6hAApDa1LT8",
            "F",
            "Zr6W8d1305bHTzlQs7NS36PASi",
            "Vm433C8d5OeitqXy",
            "11jGL7IyP35",
            "3UWflM",
            "qbh0oITPZC40O",
            "O0qJIVU3s3MvNhs0",
            "5",
            "l7p",
            "7Y7c0QS7FS4DK5UG3971Ku",
            "qEJV3m8P6nN0XA",
            "x3U1UkFon57",
            "s32b2qa7M913Qo",
            "43",
            "t3e49256a01B8W1DG8m37c",
            "TOry03Q7zB7A5",
            "EadeJXZe4Hhz6GwN",
            "MYNe7d7m4",
            "0RuXW5Ku42fF550e02v9",
        ]
        .iter()
        .filter_map(|&s| TaskLabel::try_from(s).ok())
        .collect::<Vec<_>>();

        let mut buf = String::new();
        let delimiter = "->";
        let _ = serialize_path(&path, &mut buf, delimiter, TaskLabel::MAX_LEN);

        let expected = vec![
            "0e928v8U8vJ8136qq->VO2JI->oNdK9v0L8HVsf->GSIDD3BBY5s92KwO92L7Z->",
            "BH9Iwo0->g0y4s->5W0m5D1586o8KM->p9T80Q3IMl4v3RVo9z1L->",
            "7o9Lffql1ByrSN6Nw9B9g8h3t->bzfX40xVStq3BmNYhz19LN->rYPfT7W9BT195uW2JLr->",
            "P5GMQsLs0pmQ71->4IX55y2Z03->x4nXd->1vRC03Gp4->XDpK->5Y5QX9Sr->",
            "HDS46bzvn4->I2a->P->we52ma8->3L606Qbq0x4xlj4504xYD5->YkIe19i7bDe4->0->",
            "Lq7NHYotR365uANzrp0->e9919B6knL38E2->uc8G->Sf1pUx1FpaC0gDQR11t->",
            "G3UIv7Nxq29Z->7xw->c->w4eAY4Xc27tl0PJ->du5->1e3->imm4->4Rqc->ha6K6->",
            "h7ygXHvs0->kl9R5Zhg8PLbg5CQ8S22n->6FxlsD8c3->BuoxppGpYOk9kdzEAELC7o9B->",
            "7pjk1WX9XDKafb9ZuMCq->eVpqqtLkx552s27A->5O->7f4o0mYisAvtN8QW4b71->",
            "Y3D4P->TzniQk0vbH6W23JNW2iv->F956Fm5iVk4I32r->jWS8W8euiV5sW8fd8S->",
            "X7jdHFfjk79B6G0z7094Ez97G8OX->532garQ3GytE->OvA48Av->78B1A7y->az->",
            "p72kp->3QCK->1sK8->z->TvJF92ZQUh->v8KK2w5u6a72cQmFVJph88->",
            "1CFEtP8k4pf8G0t->IBk2Y6g3H->aG4->47f08419eV->",
            "hV4qcwM0JWUb97yFkKfYcK75DL->RfbD1Cv6Y7ThmTVasf->Xrp12YvQnZ6->",
            "G2xe78a5mkXh0FeA->13cER4Bq7X290024->B->DrKrfJ->wz29wPI5S4->6hAApDa1LT8->",
            "F->Zr6W8d1305bHTzlQs7NS36PASi->Vm433C8d5OeitqXy->11jGL7IyP35->3UWflM->",
            "qbh0oITPZC40O->O0qJIVU3s3MvNhs0->5->l7p->7Y7c0QS7FS4DK5UG3971Ku->",
            "qEJV3m8P6nN0XA->x3U1UkFon57->s32b2qa7M913Qo->43->",
            "t3e49256a01B8W1DG8m37c->TOry03Q7zB7A5->EadeJXZe4Hhz6GwN->MYNe7d7m4->",
            "0RuXW5Ku42fF550e02v9",
        ];
        assert_eq!(buf.split_whitespace().collect::<Vec<&str>>(), expected);
    }

    pub use util::paths;

    // functions to make writing tests easier
    mod util {
        use super::*;

        pub fn analyze<'a, I, J>(
            task_orders: I,
            task_durations: J,
        ) -> Result<ScheduleAnalysis<'a>, AnalysisError<'a>>
        where
            I: IntoIterator<Item = &'a TaskOrder<'a>>,
            J: IntoIterator<Item = &'a (&'a str, Duration)>,
        {
            analyze_schedule(
                &task_orders.into_iter().cloned().collect(),
                &task_durations
                    .into_iter()
                    .map(|&(s, d)| (TaskLabel::new(s), d))
                    .collect(),
            )
        }

        pub fn labels<'a, I>(strs: I) -> Vec<TaskLabel<'a>>
        where
            I: IntoIterator<Item = &'a &'a str>,
        {
            strs.into_iter().map(|&str| TaskLabel::new(str)).collect()
        }

        pub fn paths<'a, I>(strs: I) -> Vec<Vec<TaskLabel<'a>>>
        where
            I: IntoIterator<Item = &'a &'a str>,
        {
            strs.into_iter()
                .map(|&str| {
                    str.split("->")
                        .map(|str| TaskLabel::new(str))
                        .collect::<Vec<_>>()
                })
                .collect()
        }
    }
}
