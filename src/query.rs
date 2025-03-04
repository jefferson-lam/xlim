use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::document::Document;
use crate::error::{Result, XLimError};

/// Comparison operators for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    /// Equal to
    Eq,
    /// Not equal to
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal to
    Gte,
    /// Less than
    Lt,
    /// Less than or equal to
    Lte,
    /// Contains (for strings and arrays)
    Contains,
    /// Starts with (for strings)
    StartsWith,
    /// Ends with (for strings)
    EndsWith,
    /// In (value is in array)
    In,
    /// Not in (value is not in array)
    NotIn,
}

impl ComparisonOperator {
    /// Parse a comparison operator from a string
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "=" | "==" | "eq" => Ok(Self::Eq),
            "!=" | "<>" | "ne" => Ok(Self::Ne),
            ">" | "gt" => Ok(Self::Gt),
            ">=" | "gte" => Ok(Self::Gte),
            "<" | "lt" => Ok(Self::Lt),
            "<=" | "lte" => Ok(Self::Lte),
            "contains" => Ok(Self::Contains),
            "startsWith" | "starts_with" => Ok(Self::StartsWith),
            "endsWith" | "ends_with" => Ok(Self::EndsWith),
            "in" => Ok(Self::In),
            "notIn" | "not_in" => Ok(Self::NotIn),
            _ => Err(XLimError::Query(format!("Invalid comparison operator: {}", s))),
        }
    }
    
    /// Apply the comparison operator to two values
    pub fn apply(&self, left: &Value, right: &Value) -> Result<bool> {
        match self {
            Self::Eq => Ok(left == right),
            Self::Ne => Ok(left != right),
            Self::Gt => compare_values(left, right, Ordering::Greater),
            Self::Gte => {
                let result = compare_values(left, right, Ordering::Greater)?;
                Ok(result || left == right)
            }
            Self::Lt => compare_values(left, right, Ordering::Less),
            Self::Lte => {
                let result = compare_values(left, right, Ordering::Less)?;
                Ok(result || left == right)
            }
            Self::Contains => apply_contains(left, right),
            Self::StartsWith => apply_starts_with(left, right),
            Self::EndsWith => apply_ends_with(left, right),
            Self::In => apply_in(left, right),
            Self::NotIn => {
                let result = apply_in(left, right)?;
                Ok(!result)
            }
        }
    }
}

/// Logical operators for combining conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalOperator {
    /// AND operator
    And,
    /// OR operator
    Or,
}

impl LogicalOperator {
    /// Parse a logical operator from a string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "and" | "&&" => Ok(Self::And),
            "or" | "||" => Ok(Self::Or),
            _ => Err(XLimError::Query(format!("Invalid logical operator: {}", s))),
        }
    }
    
    /// Apply the logical operator to two boolean values
    pub fn apply(&self, left: bool, right: bool) -> bool {
        match self {
            Self::And => left && right,
            Self::Or => left || right,
        }
    }
}

/// A condition in a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    /// Field to compare
    pub field: String,
    
    /// Comparison operator
    pub operator: ComparisonOperator,
    
    /// Value to compare against
    pub value: Value,
}

impl Condition {
    /// Create a new condition
    pub fn new<T: Into<Value>>(field: &str, operator: ComparisonOperator, value: T) -> Self {
        Self {
            field: field.to_string(),
            operator,
            value: value.into(),
        }
    }
    
    /// Check if a document matches the condition
    pub fn matches(&self, document: &Document) -> Result<bool> {
        let field_value = document.get(&self.field);
        
        match field_value {
            Some(value) => self.operator.apply(value, &self.value),
            None => Ok(false),
        }
    }
}

/// A query for filtering documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    /// Conditions to apply
    pub conditions: Vec<Condition>,
    
    /// Logical operators to combine conditions
    pub operators: Vec<LogicalOperator>,
    
    /// Fields to sort by
    pub sort: Vec<(String, bool)>, // (field, ascending)
    
    /// Maximum number of results to return
    pub limit: Option<usize>,
    
    /// Number of results to skip
    pub skip: Option<usize>,
    
    /// Fields to include in the results
    pub projection: Option<Vec<String>>,
}

impl Query {
    /// Create a new empty query
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            operators: Vec::new(),
            sort: Vec::new(),
            limit: None,
            skip: None,
            projection: None,
        }
    }
    
    /// Add a condition to the query
    pub fn filter<T: Into<Value>>(mut self, field: &str, operator: &str, value: T) -> Result<Self> {
        let operator = ComparisonOperator::from_str(operator)?;
        let condition = Condition::new(field, operator, value);
        
        if !self.conditions.is_empty() && self.operators.len() < self.conditions.len() {
            // Default to AND if no operator is specified
            self.operators.push(LogicalOperator::And);
        }
        
        self.conditions.push(condition);
        
        Ok(self)
    }
    
    /// Add a logical operator to the query
    pub fn logical_operator(mut self, operator: &str) -> Result<Self> {
        let operator = LogicalOperator::from_str(operator)?;
        
        if self.conditions.is_empty() {
            return Err(XLimError::Query("Cannot add logical operator before any conditions".to_string()));
        }
        
        if self.operators.len() >= self.conditions.len() - 1 {
            return Err(XLimError::Query("Too many logical operators".to_string()));
        }
        
        self.operators.push(operator);
        
        Ok(self)
    }
    
    /// Add a sort field to the query
    pub fn sort(mut self, field: &str, ascending: bool) -> Self {
        self.sort.push((field.to_string(), ascending));
        self
    }
    
    /// Set the maximum number of results to return
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    
    /// Set the number of results to skip
    pub fn skip(mut self, skip: usize) -> Self {
        self.skip = Some(skip);
        self
    }
    
    /// Set the fields to include in the results
    pub fn project(mut self, fields: Vec<&str>) -> Self {
        self.projection = Some(fields.iter().map(|f| f.to_string()).collect());
        self
    }
    
    /// Check if a document matches the query
    pub fn matches(&self, document: &Document) -> Result<bool> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        
        let mut result = self.conditions[0].matches(document)?;
        
        for i in 1..self.conditions.len() {
            let condition_result = self.conditions[i].matches(document)?;
            let operator = &self.operators[i - 1];
            
            result = operator.apply(result, condition_result);
        }
        
        Ok(result)
    }
    
    /// Apply the query to a list of documents
    pub fn apply(&self, documents: Vec<Document>) -> Result<Vec<Document>> {
        // Filter documents
        let mut results: Vec<Document> = documents
            .into_iter()
            .filter(|doc| self.matches(doc).unwrap_or(false))
            .collect();
        
        // Sort documents
        if !self.sort.is_empty() {
            results.sort_by(|a, b| {
                for (field, ascending) in &self.sort {
                    if let (Some(a_val), Some(b_val)) = (a.get(field), b.get(field)) {
                        let cmp = compare_json_values(a_val, b_val);
                        
                        if cmp != Ordering::Equal {
                            return if *ascending { cmp } else { cmp.reverse() };
                        }
                    }
                }
                
                Ordering::Equal
            });
        }
        
        // Skip documents
        if let Some(skip) = self.skip {
            if skip < results.len() {
                results = results.into_iter().skip(skip).collect();
            } else {
                results.clear();
            }
        }
        
        // Limit documents
        if let Some(limit) = self.limit {
            if limit < results.len() {
                results.truncate(limit);
            }
        }
        
        // Apply projection
        if let Some(projection) = &self.projection {
            results = results
                .into_iter()
                .map(|doc| {
                    let mut new_doc = Document::new();
                    new_doc.id = doc.id;
                    new_doc.created_at = doc.created_at;
                    new_doc.updated_at = doc.updated_at;
                    
                    for field in projection {
                        if let Some(value) = doc.get(field) {
                            new_doc.data.insert(field.clone(), value.clone());
                        }
                    }
                    
                    new_doc
                })
                .collect();
        }
        
        Ok(results)
    }
}

/// A query builder for creating queries
pub struct QueryBuilder {
    query: Query,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            query: Query::new(),
        }
    }
    
    /// Add a filter condition to the query
    pub fn filter<T: Into<Value>>(&mut self, field: &str, operator: &str, value: T) -> Result<&mut Self> {
        self.query = self.query.filter(field, operator, value)?;
        Ok(self)
    }
    
    /// Add a logical operator to the query
    pub fn logical_operator(&mut self, operator: &str) -> Result<&mut Self> {
        self.query = self.query.logical_operator(operator)?;
        Ok(self)
    }
    
    /// Add a sort field to the query
    pub fn sort(&mut self, field: &str, ascending: bool) -> &mut Self {
        self.query = self.query.sort(field, ascending);
        self
    }
    
    /// Set the maximum number of results to return
    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.query = self.query.limit(limit);
        self
    }
    
    /// Set the number of results to skip
    pub fn skip(&mut self, skip: usize) -> &mut Self {
        self.query = self.query.skip(skip);
        self
    }
    
    /// Set the fields to include in the results
    pub fn project(&mut self, fields: Vec<&str>) -> &mut Self {
        self.query = self.query.project(fields);
        self
    }
    
    /// Build the query
    pub fn build(&self) -> Query {
        self.query.clone()
    }
}

// Helper functions for comparison operations

fn compare_values(left: &Value, right: &Value, expected: Ordering) -> Result<bool> {
    let ordering = compare_json_values(left, right);
    
    if ordering == Ordering::Equal {
        return Ok(false);
    }
    
    Ok(ordering == expected)
}

fn compare_json_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a_f64), Some(b_f64)) = (a.as_f64(), b.as_f64()) {
                a_f64.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
            } else {
                Ordering::Equal
            }
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => {
            let len_cmp = a.len().cmp(&b.len());
            
            if len_cmp != Ordering::Equal {
                return len_cmp;
            }
            
            for (a_val, b_val) in a.iter().zip(b.iter()) {
                let cmp = compare_json_values(a_val, b_val);
                
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            
            Ordering::Equal
        }
        (Value::Object(a), Value::Object(b)) => {
            let len_cmp = a.len().cmp(&b.len());
            
            if len_cmp != Ordering::Equal {
                return len_cmp;
            }
            
            for (key, a_val) in a {
                if let Some(b_val) = b.get(key) {
                    let cmp = compare_json_values(a_val, b_val);
                    
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                } else {
                    return Ordering::Greater;
                }
            }
            
            Ordering::Equal
        }
        // Different types
        (Value::Bool(_), _) => Ordering::Less,
        (_, Value::Bool(_)) => Ordering::Greater,
        (Value::Number(_), _) => Ordering::Less,
        (_, Value::Number(_)) => Ordering::Greater,
        (Value::String(_), _) => Ordering::Less,
        (_, Value::String(_)) => Ordering::Greater,
        (Value::Array(_), _) => Ordering::Less,
        (_, Value::Array(_)) => Ordering::Greater,
    }
}

fn apply_contains(left: &Value, right: &Value) -> Result<bool> {
    match (left, right) {
        (Value::String(a), Value::String(b)) => Ok(a.contains(b)),
        (Value::Array(a), b) => Ok(a.contains(b)),
        _ => Err(XLimError::Query("Contains operator can only be applied to strings and arrays".to_string())),
    }
}

fn apply_starts_with(left: &Value, right: &Value) -> Result<bool> {
    match (left, right) {
        (Value::String(a), Value::String(b)) => Ok(a.starts_with(b)),
        _ => Err(XLimError::Query("StartsWith operator can only be applied to strings".to_string())),
    }
}

fn apply_ends_with(left: &Value, right: &Value) -> Result<bool> {
    match (left, right) {
        (Value::String(a), Value::String(b)) => Ok(a.ends_with(b)),
        _ => Err(XLimError::Query("EndsWith operator can only be applied to strings".to_string())),
    }
}

fn apply_in(left: &Value, right: &Value) -> Result<bool> {
    match right {
        Value::Array(arr) => Ok(arr.contains(left)),
        _ => Err(XLimError::Query("In operator requires an array as the right operand".to_string())),
    }
} 