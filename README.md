# Task Schedule Analyzer

Sometimes we need to execute many small tasks in a particular order to consider a job "done". 
This application takes as input a set of tasks that make up a directed-acyclic-graph(DAG) and renders an 
analysis of the schedule with the assumption that infinite resources are available to execute the tasks. 
The analyzer is also capable of dealing with multiple DAGs in the input file.

![Build Status](https://github.com/onurzdg/analyze-task-schedule/actions/workflows/rust.yml/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

## Input and Output

### Input: 
A file containing tasks, their non-negative durations and dependencies. 
Bad input, missing information, and cycles will be detected and rejected with an appropriate error message.
Parser is capable of dealing with files that have an inconsistent amount white spaces between each token.

### Output:
task_count: number of tasks found in the input file
max_parallelism: maximum number of tasks that can be found executing simultaneously
minimum_completion_time: minimum time to execute all tasks, which is the same as the time it takes to complete the
critical path
critical_path_count: number of discovered critical paths
critical_paths: all discovered critical paths

## Sample Input File Format

In the sample input file below, "Q(1)" is a task that takes one unit of time to execute.
"T(1) after [Q]" means "T" takes one unit to execute and has to be executed after "A".

Given this input file
```
Q(1)
T(1) after [Q]
J(1)
  after [Q]
K(1) after [T]
N(1) after
  [T,
   J]
P(1) after [J]
H(1) after [K, N]
I(1) after
  [N, P]
```

the output will be

```
task_count: 8
max_parallelism: 3
minimum_completion_time: 4
critical_path_count: 6
critical_paths:
1)
Q->J->N->H

2)
Q->J->N->I

3)
Q->J->P->I

4)
Q->T->K->H

5)
Q->T->N->H

6)
Q->T->N->I
```

For further input and output samples, check `resources/test` folder.
See `src/schedule.pest` for complete input file grammar.

## Execution

```bash
cargo run file_path
```
