// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// ChaosExec is a physical execution plan node for robustness/chaos testing.
// It wraps a single child node, preserving its schema and partitioning, and
// randomly injects faults according to a configurable failure_probability
// (in [0.0, 1.0]) and fault_type:
//
//   "transient"  — returns a recoverable IoError on the first batch
//   "fatal"      — returns a non-recoverable Execution error on the first batch
//   "panic"      — panics on the first batch
//   "delay"      — sleeps 1 ms before every batch
//   "delay:N"    — sleeps N ms before every batch
//
// ChaosExec is inserted into query plans by physical optimizer rule, which
// probabilistically wraps leaf nodes.

use datafusion::common::{DataFusionError, Result, Statistics, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use std::any::Any;
use std::sync::Arc;

/// Physical execution plan node that randomly injects failures for chaos/robustness testing.
#[derive(Debug, Clone)]
pub struct ChaosExec {
    input: Arc<dyn ExecutionPlan>,
    // a probability this node will fail when run
    failure_probability: f64,
    // controls what kind of fault is injected: "transient", "fatal", "panic", or "delay"
    fault_type: String,
}

impl ChaosExec {
    /// Creates a new `ChaosExec` wrapping `input`.
    ///
    /// `failure_probability` must be in `[0.0, 1.0]`.
    /// `fault_type` must be one of `"transient"`, `"fatal"`, `"panic"`, `"delay"`, or `"delay:N"`
    /// where N is a positive integer number of milliseconds.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        failure_probability: f64,
        fault_type: &str,
    ) -> Result<Self> {
        if !(0.0..=1.0).contains(&failure_probability) {
            return internal_err!(
                "ChaosExec failure_probability must be in [0.0, 1.0], got {failure_probability}"
            );
        }
        match fault_type {
            "transient" | "fatal" | "panic" | "delay" => {}
            other if other.starts_with("delay:") => {
                let ms_str = &other["delay:".len()..];
                if ms_str.parse::<u64>().is_err() {
                    return internal_err!(
                        "ChaosExec delay suffix must be a positive integer (ms), got \"{ms_str}\""
                    );
                }
            }
            other => {
                return internal_err!(
                    "ChaosExec fault_type must be one of transient/fatal/panic/delay/delay:N, got {other}"
                );
            }
        }

        Ok(Self {
            input,
            failure_probability,
            fault_type: fault_type.to_string(),
        })
    }

    /// Returns the configured failure probability.
    pub fn failure_probability(&self) -> f64 {
        self.failure_probability
    }

    /// Returns the configured fault type.
    pub fn fault_type(&self) -> &str {
        &self.fault_type
    }
}

impl ExecutionPlan for ChaosExec {
    fn name(&self) -> &str {
        "ChaosExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("ChaosExec expected one child, got {}", children.len());
        }
        let new_input = children.pop().unwrap();
        Ok(Arc::new(Self::new(
            new_input,
            self.failure_probability,
            &self.fault_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let failure_probability = self.failure_probability;
        let fault_type = self.fault_type.clone();
        let schema = self.input.schema();

        // Wrap the child stream. For error/panic modes, inject on the first batch (idx == 0)
        // to mirror how real IO failures surface in production. For "delay", sleep every batch.
        let wrapped = input_stream.enumerate().map(move |(idx, batch_result)| {
            match fault_type.to_lowercase().as_str() {
                ft if ft.starts_with("delay") => {
                    std::thread::sleep(std::time::Duration::from_millis(parse_delay_ms(ft)));
                    batch_result
                }
                "transient" if idx == 0 && rand::random::<f64>() < failure_probability => {
                    let error_msg = format!(
                        "ChaosExec: injected transient failure on partition {partition}"
                    );
                    log::error!("{}",error_msg);
                    Err(DataFusionError::IoError(std::io::Error::other(error_msg)))
                }
                "transient" => batch_result,

                "fatal" if idx == 0 && rand::random::<f64>() < failure_probability => {
                    let error_msg = format!(
                        "ChaosExec: injected fatal failure on partition {partition} (chaos testing)"
                    );
                    log::error!("{}",error_msg);
                    Err(DataFusionError::Execution(error_msg))
                }
                "panic" if idx == 0 && rand::random::<f64>() < failure_probability => {
                    log::error!("ChaosExec: injected panic on partition {partition}");
                    panic!("ChaosExec: injected panic on partition {partition}")
                }
                config => {
                    log::error!("ChaosExec: wrong config value {config}, will break execution anyway");
                    panic!("ChaosExec: wrong config value {config}, will break execution anyway")
                },
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, wrapped)))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.input.cardinality_effect()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new_input = self.input.with_fetch(limit)?;
        Some(Arc::new(
            Self::new(new_input, self.failure_probability, &self.fault_type).ok()?,
        ))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(new_input) = self.input.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(Self::new(
                new_input,
                self.failure_probability,
                &self.fault_type,
            )?)))
        } else {
            Ok(None)
        }
    }

    fn with_preserve_order(
        &self,
        preserve_order: bool,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let new_input = self.input.with_preserve_order(preserve_order)?;
        Some(Arc::new(
            Self::new(new_input, self.failure_probability, &self.fault_type).ok()?,
        ))
    }
}

fn parse_delay_ms(fault_type: &str) -> u64 {
    fault_type
        .strip_prefix("delay:")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

impl DisplayAs for ChaosExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ChaosExec: failure_probability={}, fault_type={}",
                    self.failure_probability, self.fault_type
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "failure_probability={}, fault_type={}",
                    self.failure_probability, self.fault_type
                )
            }
        }
    }
}
