use std::convert::TryFrom;
use std::fmt;
use std::fmt::Formatter;
use std::ops::Deref;

pub type Duration = u16;
pub type TotalDuration = u32;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskLabel<'a>(&'a str);
impl<'a> Deref for TaskLabel<'a> {
    type Target = str;

    fn deref(&self) -> &'a str {
        self.0
    }
}

impl<'a> TaskLabel<'a> {
    pub const MAX_LEN: usize = 70;
    /// Whitespace characters, strings that exceed the MAX_LEN limit, and empty strings will result
    /// in a panic!
    pub fn new(s: &'a str) -> Self {
        match TaskLabel::try_from(s) {
            Ok(label) => label,
            Err(err) => panic!(err),
        }
    }
}

impl<'a> AsRef<str> for TaskLabel<'a> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl<'a> TryFrom<&'a str> for TaskLabel<'a> {
    type Error = String;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        if s.is_empty() {
            Err(String::from("Empty strings cannot be labels"))
        } else if s.chars().count() > TaskLabel::MAX_LEN {
            Err(format!(
                "Labels cannot have more than {} characters: {}",
                TaskLabel::MAX_LEN,
                s
            ))
        } else if s.contains(char::is_whitespace) {
            Err(format!("Labels cannot have whitespace characters: {}", s))
        } else {
            Ok(TaskLabel(s))
        }
    }
}

impl<'a> TryFrom<&'a String> for TaskLabel<'a> {
    type Error = String;

    fn try_from(s: &'a String) -> Result<Self, Self::Error> {
        TaskLabel::try_from(s.as_str())
    }
}

impl<'a> std::fmt::Debug for TaskLabel<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TL({})", self.0)
    }
}

impl<'a> std::fmt::Display for TaskLabel<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TaskLabel({})", self.0)
    }
}

/// Convenience trait
pub trait TaskRelation<'a, R> {
    /// Self points to "right". Arrow as in A points to B. That is, A --> B.
    /// Cyclic relations will result in a panic.
    fn arrow(self, right: R) -> TaskOrder<'a>;

    /// Can be a path with a single node unless later fused with another node
    fn node(self) -> TaskOrder<'a>;
}

impl<'a> TaskRelation<'a, TaskLabel<'a>> for TaskLabel<'a> {
    fn arrow(self, right: TaskLabel<'a>) -> TaskOrder<'a> {
        if self == right {
            panic!("Labels cannot have a dependency on themselves: {}", self)
        }
        TaskOrder {
            first: self,
            second: right.into(),
        }
    }

    fn node(self) -> TaskOrder<'a> {
        TaskOrder {
            first: self,
            second: None,
        }
    }
}

impl<'a> TaskRelation<'a, &'a str> for &'a str {
    fn arrow(self, right: &'a str) -> TaskOrder<'a> {
        TaskLabel::new(self).arrow(TaskLabel::new(right))
    }

    fn node(self) -> TaskOrder<'a> {
        TaskLabel::new(self).node()
    }
}

/// Clarifies the order/dependence between two tasks.
/// Absence of second indicates that first task is not a prerequisite
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct TaskOrder<'a> {
    first: TaskLabel<'a>,
    second: Option<TaskLabel<'a>>,
}

impl<'a> TaskOrder<'a> {
    pub fn first(&self) -> TaskLabel<'a> {
        self.first
    }

    pub fn second(&self) -> Option<TaskLabel<'a>> {
        self.second
    }
}

impl<'a> TaskOrder<'a> {
    #[allow(dead_code)]
    pub fn is_node(&self) -> bool {
        self.second.is_none()
    }
}

impl<'a> std::fmt::Display for TaskOrder<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "first: {}, second: {:?}", self.first, self.second)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::TestResult;

    #[quickcheck]
    fn attempt_to_form_cyclic_dependency(s: String) -> TestResult {
        TestResult::must_fail(move || {
            let str = s.as_str();
            str.arrow(str);
        })
    }

    #[quickcheck]
    fn test_attempt_to_use_empty_label_left_side_of_arrow(s: String) -> TestResult {
        // make sure the right label is good
        if TaskLabel::try_from(s.as_str()).is_ok() {
            return TestResult::discard();
        }
        TestResult::must_fail(move || {
            "".arrow(&s);
        })
    }

    #[quickcheck]
    fn attempt_to_use_empty_label_right_side_of_arrow(s: String) -> TestResult {
        // make sure the left label is good
        if TaskLabel::try_from(s.as_str()).is_ok() {
            return TestResult::discard();
        }
        TestResult::must_fail(move || {
            s.as_str().arrow("");
        })
    }

    #[test]
    #[should_panic]
    fn attempt_to_use_empty_label_both_sides_of_arrow() {
        "".arrow("");
    }

    #[quickcheck]
    fn attempt_to_use_white_space_in_label(s: String) -> TestResult {
        if !s.contains(char::is_whitespace) {
            return TestResult::discard();
        }
        TestResult::must_fail(move || {
            TaskLabel::new(&s);
        })
    }

    #[quickcheck]
    fn attempt_to_exceed_label_char_limit(s: String) -> TestResult {
        if s.chars().count() <= TaskLabel::MAX_LEN {
            return TestResult::discard();
        }
        TestResult::must_fail(move || {
            TaskLabel::new(&s);
        })
    }
}
